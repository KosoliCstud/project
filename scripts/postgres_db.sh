sudo systemctl restart postgresql
sudo -u postgres psql -c "drop DATABASE gios_data;"
sudo -u postgres psql -c "create DATABASE gios_data;"