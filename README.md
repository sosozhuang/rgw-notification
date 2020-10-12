RGW Notification
======

### Installation
1. Clone the repository from Github.
```bash
git clone https://github.com/sosozhuang/rgw-notification.git
```
2. Build and package binaries.
```bash
mvn clean package
```
3. Run notification service.
```bash
cd dist && tar zxvf rgw-notification-1.0.0.tar.gz
cd rgw-notification-1.0.0
bin/start.sh
```

License
-------
Code is licensed under the [Apache License 2.0](https://github.com/sosozhuang/rgw-client/blob/master/LICENSE).