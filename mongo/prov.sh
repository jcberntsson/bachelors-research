
# Install MySQL Server
echo "Provisioning"
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927

echo "deb http://repo.mongodb.org/apt/ubuntu "$(lsb_release -sc)"/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb.list

sudo apt-get update
sudo apt-get install -y mongodb-org

echo "Fixing mongo binding"
sed -i "/bindIp/d" /etc/mongod.conf
sudo service mongod restart

echo "Mongo Version"
mongod --version

echo "Done provisioning"