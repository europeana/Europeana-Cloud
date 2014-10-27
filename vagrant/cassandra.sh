# cassandra.sh

#install cassandra
#
#add the following lines /etc/apt/sources.list
if ! grep -qe "dist/cassandra/debian" "/etc/apt/sources.list"; then
	echo "
deb http://www.apache.org/dist/cassandra/debian 20x main  
deb-src http://www.apache.org/dist/cassandra/debian 20x main  
" | sudo tee -a /etc/apt/sources.list
else
    echo "cassandra sources already in list"
fi


# install (the unverified since no public key set) cassandra package
sudo apt-get update -yq --fix-missing

## grub causes a slpash screen that breaks upgrade
# so do Not updgrade
echo "grub-common hold" | sudo dpkg --set-selections
echo "grub-pc hold" | sudo dpkg --set-selections
echo "grub-pc-bin hold" | sudo dpkg --set-selections
echo "grub2-common hold" | sudo dpkg --set-selections

sudo apt-get upgrade -f -y --force-yes
sudo apt-get install -y --force-yes cassandra

# wget http://www.apache.org/dist/cassandra/debian/pool/main/c/cassandra/cassandra_2.0.8_all.deb

# configure 
sudo mkdir -p /data/cassandra/datafile
sudo mkdir -p /data/cassandra/commitlog
sudo mkdir -p /data/cassandra/caches

# point to the above directories in yaml file
sudo sed -i 's/\/var\/lib\/cassandra\/data/\/data\/cassandra\/datafile/' /etc/cassandra/cassandra.yaml 
sudo sed -i 's/\/var\/lib\/cassandra\/commitlog/\/data\/cassandra\/commitlog/' /etc/cassandra/cassandra.yaml 
sudo sed -i 's/\/var\/lib\/cassandra\/saved_caches/\/data\/cassandra\/caches/' /etc/cassandra/cassandra.yaml 
# also point to log4j props
sudo sed -i 's/log4j.appender.R.File=\/var\/log\/cassandra\/system.log/log4j.appender.R.File=\/data\/cassandra\/system.log/' /etc/cassandra/log4j-server.properties

sudo chown -R cassandra:cassandra /etc/cassandra 
sudo chown -R cassandra:cassandra /data/cassandra 

# change in /etc/cassandra/cassandra.yaml
# authenticator: AllowAllAuthenticator    |---->
# authenticator: PasswordAuthenticator
sudo sed -i 's/authenticator: AllowAllAuthenticator/authenticator: PasswordAuthenticator/' /etc/cassandra/cassandra.yaml 

# also
# authorizer: AllowAllAuthorizer          |---->
# authorizer: CassandraAuthorizer
sudo sed -i 's/authorizer: AllowAllAuthorizer/authorizer: CassandraAuthorizer/' /etc/cassandra/cassandra.yaml 

#restart service
sudo /etc/init.d/cassandra stop
sudo /etc/init.d/cassandra start

sleep 5;

#create schema
cqlsh -u cassandra -p cassandra -f /vagrant/conf/auth.cql
#create users and grant permissions
cqlsh -u cassandra -p cassandra -f /vagrant/conf/schema.cql


## install zookeeper
cd
curl -O http://ftp.nluug.nl/internet/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
tar -xzf zookeeper-3.4.6.tar.gz
cd zookeeper-3.4.6

sudo cp /vagrant/conf/zoo.cfg ./conf/zoo.cfg
touch /var/zookeeper/myid
echo -e "2" > /var/zookeeper/myid
sudo bin/zkServer.sh start

