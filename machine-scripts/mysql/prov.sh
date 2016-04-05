
# Install MySQL Server
echo "Provisioning"
debconf-set-selections <<< 'mysql-server mysql-server/root_password password vagrant'
debconf-set-selections <<< 'mysql-server mysql-server/root_password_again password vagrant'

echo "Installing mysql"
apt-get update
apt-get install -y mysql-server
echo "Mysql installed"

echo "Restarting mysql"
sudo service mysql restart
echo "Mysql restarted"

echo "Configuring MySQL"
echo "CREATE DATABASE research" | mysql -uroot -pvagrant
echo "CREATE USER 'vagrant'@'%' IDENTIFIED BY 'vagrant'" | mysql -uroot -pvagrant
echo "GRANT ALL ON *.* TO 'vagrant'@'%'" | mysql -uroot -pvagrant
echo "GRANT ALL ON *.* TO 'root'@'%'" | mysql -uroot -pvagrant
echo "flush privileges" | mysql -uroot -pvagrant

sed -i "s/^bind-address/#bind-address/" /etc/mysql/my.cnf
sed -i "s/^skip-external-locking/#skip-external-locking/" /etc/mysql/my.cnf

sudo service mysql restart
echo "MySQL done"
