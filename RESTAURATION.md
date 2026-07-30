# Procédure de restauration — VPS Docker (WordPress + Nginx Proxy Manager)

Cette procédure restaure un VPS à partir d'une sauvegarde produite par `backup.py`. La sauvegarde est **ciblée** (pas un système complet) : elle contient les volumes Docker, `/opt/wordpress`, et la configuration système (ufw, fail2ban, ssh, cron). Restaurer consiste donc à reconstruire un VPS neuf avec les mêmes paquets et la même structure Docker, puis à réinjecter ces données par-dessus — pas à écraser un disque entier.

À garder sous la main pendant l'opération : l'accès au bucket S3 (`config.toml` + `/root/.aws/credentials`), la clé SSH du VPS, et les noms de domaine pointés sur l'ancien VPS.

## Contenu réel d'une sauvegarde

| Élément | Origine | Rôle |
|---|---|---|
| `dev_db_data` | volume Docker | Base de données MariaDB du site |
| `nginx-proxy_data` | volume Docker | Config des Proxy Hosts de NPM |
| `nginx-proxy_letsencrypt` | volume Docker | Certificats SSL Let's Encrypt |
| `/opt/wordpress` | dossier | Fichiers WordPress + `docker-compose.yml` / `.env` |
| `/etc/ufw` | dossier | Règles du pare-feu |
| `/etc/fail2ban` | dossier | Config anti-bruteforce (si personnalisée) |
| `/etc/ssh/sshd_config` | fichier | Durcissement SSH |
| `/etc/cron.d` | dossier | Tâches planifiées système |
| `/etc/systemd/system/vps-backup.*` | fichiers | Timer de sauvegarde lui-même |
| `packages.list` | généré à chaque run | `dpkg --get-selections`, pour réinstaller les mêmes paquets |

## Étape 1 — Provisionner le nouveau VPS

Installe un Debian propre (même version que l'ancien VPS), connecte-toi en root, et vérifie l'accès réseau de base avant de continuer.

```bash
apt update && apt upgrade -y
```

## Étape 2 — Réinstaller les paquets système

Récupère `packages.list` depuis la sauvegarde (voir étape 3 pour l'extraire), puis :

```bash
dpkg --set-selections < packages.list
apt-get dselect-upgrade -y
```

Cette commande réinstalle Docker, ufw, fail2ban et tout ce qui était présent sur l'ancien VPS, sans avoir eu besoin de copier le moindre binaire. Si Docker n'est pas repris automatiquement (paquet non géré par apt selon ton installation d'origine), installe-le manuellement :

```bash
curl -fsSL https://get.docker.com | sh
```

## Étape 3 — Récupérer la sauvegarde

Installe `vps-backup-simple` sur le nouveau VPS (mêmes fichiers `backup.py`, `restore.py`, `config.toml`, credentials S3 dans `/root/.aws/credentials`).

```bash
python3 restore.py list --config config.toml
```

Repère la clé de la dernière sauvegarde valide (colonne "Cle S3"), puis restaure dans un dossier temporaire de staging :

```bash
python3 restore.py restore vps-debian/vps-backup-<date>.tar.gz /mnt/restore --config config.toml
```

La vérification SHA256 est automatique (`--no-verify` pour la désactiver, déconseillé). En cas d'échec d'intégrité, la restauration s'arrête — ne continue pas la procédure avec une sauvegarde corrompue, essaie la sauvegarde précédente (`retention` en garde plusieurs).

Le contenu atterrit sous ses chemins absolus d'origine, par exemple :

```
/mnt/restore/opt/wordpress/...
/mnt/restore/etc/ufw/...
/mnt/restore/var/lib/docker/volumes/dev_db_data/_data/...
```

## Étape 4 — Recréer les volumes Docker AVANT de réinjecter les données

Il faut que Docker connaisse les volumes avant d'y déposer des fichiers, sinon Docker recrée le dossier vide au prochain démarrage et écrase ce qui vient d'être restauré.

```bash
docker volume create dev_db_data
docker volume create nginx-proxy_data
docker volume create nginx-proxy_letsencrypt
```

Cela crée les mountpoints (`docker volume inspect <nom> --format '{{ .Mountpoint }}'`) — normalement identiques à l'ancien VPS (`/var/lib/docker/volumes/<nom>/_data`) tant que Docker utilise sa configuration par défaut.

## Étape 5 — Réinjecter les fichiers restaurés à leur emplacement réel

```bash
rsync -a /mnt/restore/opt/wordpress/ /opt/wordpress/
rsync -a /mnt/restore/etc/ufw/ /etc/ufw/
rsync -a /mnt/restore/etc/fail2ban/ /etc/fail2ban/ 2>/dev/null || true
cp /mnt/restore/etc/ssh/sshd_config /etc/ssh/sshd_config
rsync -a /mnt/restore/etc/cron.d/ /etc/cron.d/

for vol in dev_db_data nginx-proxy_data nginx-proxy_letsencrypt; do
  mp=$(docker volume inspect "$vol" --format '{{ .Mountpoint }}')
  rsync -a "/mnt/restore/var/lib/docker/volumes/$vol/_data/" "$mp/"
done
```

`rsync -a` préserve les permissions et propriétaires — important pour MariaDB (l'utilisateur `mysql` à l'intérieur du conteneur doit retrouver ses fichiers avec les bons droits).

## Étape 6 — Redémarrer les services

```bash
systemctl restart ssh
ufw enable
systemctl restart fail2ban 2>/dev/null || true

cd /opt/wordpress
docker compose up -d
```

Vérifie que NPM redémarre bien avec sa config et ses certs restaurés (pas de nouvelle demande Let's Encrypt nécessaire — évite de consommer le rate limit de 5 échecs/heure/domaine).

## Étape 7 — Repointer le DNS et vérifier

1. Mets à jour l'enregistrement A du domaine chez OVH vers l'IP du nouveau VPS.
2. Teste avant bascule DNS via une entrée `hosts` locale (`IP_NOUVEAU_VPS tondomaine.fr`) pour vérifier que le site répond correctement en HTTPS.
3. Vérifie `docker compose ps` (tous les conteneurs `Up`), les logs (`docker compose logs -f`), et l'accès admin WordPress.
4. Vérifie `ufw status verbose` et confirme que `sshd_config` restauré n'a pas cassé ta connexion (teste dans une **deuxième session SSH** avant de fermer la première, au cas où).
5. Une fois validé, bascule le DNS et retire la ligne `hosts` de test.

## Points d'attention

Garde l'ancien VPS actif quelques jours après la bascule pour un rollback rapide (il suffit de repointer le DNS en arrière). Si un volume Docker a un nom légèrement différent après recréation (ex. préfixe de projet Compose changé), corrige `docker_volumes` dans `config.toml` avant la prochaine sauvegarde — sinon `backup.py check` te le signalera de toute façon via la vérification "Résolution des sources". Cette procédure suppose un VPS Debian de version équivalente à l'original ; une montée de version majeure entre-temps peut faire échouer `apt-get dselect-upgrade` sur certains paquets, à traiter au cas par cas.
