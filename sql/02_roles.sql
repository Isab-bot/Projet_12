-- Rôles applicatifs : principe du moindre privilège
-- Les mots de passe sont lus dans les variables d'environnement du conteneur
\getenv loader_password POSTGRES_LOADER_PASSWORD
\getenv reader_password POSTGRES_READER_PASSWORD

CREATE ROLE checkit_loader LOGIN PASSWORD :'loader_password';
CREATE ROLE checkit_reader LOGIN PASSWORD :'reader_password';

GRANT SELECT, INSERT, UPDATE ON publications TO checkit_loader;
GRANT SELECT ON publications TO checkit_reader;