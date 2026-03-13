# Projet Enron

## Prérequis

Avant de commencer, assurez-vous que Docker est bien lancé sur votre machine.

## Démarrage du projet

Lancez les conteneurs Docker avec la commande suivante :

```bash
docker compose up
```

## Migrations de la base de données

Une fois les conteneurs démarrés, appliquez les migrations de la base de données :

```bash
docker compose exec web python manage.py makemigrations
docker compose exec web python manage.py migrate
```

## Téléchargement des données Enron

Téléchargez ensuite les données Enron dans le dossier `data` :

```bash
docker compose exec web python manage.py download_data
```

## Import des emails dans la base de données

Pour importer les emails Enron dans la base de données, utilisez la commande suivante :

```bash
docker compose exec web python manage.py import_enron data/enron/maildir
```

### Paramètres optionnels

Vous pouvez limiter l'import avec les paramètres suivants :

- `--max-mailboxes`
- `--max-files-per-mailbox`

Exemple :

```bash
docker compose exec web python manage.py import_enron data/enron/maildir --max-mailboxes 10 --max-files-per-mailbox 100
```

## Inférence des collaborateurs et des alias

Pour inférer automatiquement les collaborateurs et leurs alias à partir des emails importés :

```bash
docker compose exec web python manage.py infer_collaborators data/enron/maildir
```

### Paramètres optionnels

- `--max-mailboxes`
- `--max-files-per-mailbox`

Exemple :

```bash
docker compose exec web python manage.py infer_collaborators data/enron/maildir --max-mailboxes 10 --max-files-per-mailbox 100
```

## Inférence des alias uniquement

Si les collaborateurs sont déjà enregistrés en base de données mais que leurs alias ne sont pas encore renseignés, utilisez la commande suivante :

```bash
docker compose exec web python manage.py infer_alias data/enron/maildir
```

### Paramètres optionnels

- `--max-mailboxes`
- `--max-files-per-mailbox`

Exemple :

```bash
docker compose exec web python manage.py infer_alias data/enron/maildir --max-mailboxes 10 --max-files-per-mailbox 100
```

## Résumé des commandes

```bash
docker compose up
docker compose exec web python manage.py makemigrations
docker compose exec web python manage.py migrate
docker compose exec web python manage.py download_data
docker compose exec web python manage.py import_enron data/enron/maildir
docker compose exec web python manage.py infer_collaborators data/enron/maildir
docker compose exec web python manage.py infer_alias data/enron/maildir
```
