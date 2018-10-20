dep ensure
go install imho-backend/

# set -a
# . ./.env
# set +a

sudo cp supervisor.conf /etc/supervisor/conf.d/imho-backend.conf
supervisorctl reload