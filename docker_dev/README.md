## Docker Development Environment for WOFpy

To run the wofpy docker development simply go to the root repository and run the commands below. (Assuming Docker and Docker compose have been installed.)

Startup server:

```bash
sudo docker-compose -f docker_dev/docker-compose.yml -p wofpy up
```

Teardown server:

```bash
sudo docker-compose -f docker_dev/docker-compose.yml -p wofpy down --volumes
```

NOTE: No data has been loaded into the mysql database!