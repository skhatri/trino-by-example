#create CA cert and self sign
if [[ ! -d ca ]];
then
  mkdir ca
  openssl genrsa -out ca/ca.key 2048
  openssl req -new -x509 -key ca/ca.key -out ca/ca.crt -subj "/C=AU/ST=NSW/L=Sydney/O=Software Company/OU=IT/CN=typeboot"
fi;


host_name=${1:-localhost}
alt_name=$2

if [[ $host_name == "ca" ]];
then
  echo "invalid server name"
  exit 1;
fi;

cert=certs/$host_name
if [[ ! -d $cert ]];
then
  mkdir "$cert"
fi;
ext="san=dns:${host_name}"
if [[ ! -z ${alt_name} ]];
then
  ext="${ext},dns:${alt_name}"
fi;

### Server Setup
KEY_FILE="$cert/keystore.p12"
STORE_PASS="cassandra"
keytool -genkeypair -keyalg RSA -keysize 2048 -storetype PKCS12 -keystore ${KEY_FILE} -validity 365 \
--dname "CN=$host_name, OU=IT, O=Software Company, L=World, ST=World, C=WO" \
-ext "${ext}" \
-storepass ${STORE_PASS}

keytool -certreq -keystore "${KEY_FILE}" -file "${cert}/server.csr" -storepass ${STORE_PASS} -keypass ${STORE_PASS}


#CA sign cert request
openssl x509 -req -in $cert/server.csr -CA ca/ca.crt -CAkey ca/ca.key -CAcreateserial -out $cert/server.crt

openssl x509 -in $cert/server.crt -noout -text
cat $cert/server.crt ca/ca.crt > $cert/bundle.crt
openssl x509 -in $cert/bundle.crt -noout -text

### Server Import Cert
echo yes | keytool -importcert -file $cert/bundle.crt -keystore ${KEY_FILE} -keypass ${STORE_PASS} -storepass ${STORE_PASS} -trustcacerts
### Create Truststore
echo yes | keytool -importcert -alias cassandra -file $cert/bundle.crt -keystore $cert/truststore.jks -storepass ${STORE_PASS} -keypass ${STORE_PASS} -trustcacerts

echo extracting private key
openssl pkcs12 -info -in ${KEY_FILE} -nodes -nocerts -out $cert/server.key -password "pass:${STORE_PASS}"
openssl rsa -in $cert/server.key -out $cert/server.rsa.key
echo files generated at ${cert}
ls $cert

