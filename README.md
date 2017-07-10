# a test project for learning zookeeper

get the zookeeper docker image
```
docker pull zookeeper
```

to run the zookeeper cluster
```
# it will read the docker-compose.yml
docker-compose up
```

to run a zookeeper cli
```
# zookeeper_zoo3_1 is the name of the one of the zookeeper instances
docker run -it --rm --name zoo_client --link zookeeper_zoo3_1:zookeeper --net zookeeper_default zookeeper zkCli.sh -server zookeeper
```