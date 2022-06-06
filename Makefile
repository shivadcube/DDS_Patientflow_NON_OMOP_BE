BUILD_TOOL=/usr/local/bin/docker

gen: genrate

genrate: clean build list

clean:
	$(BUILD_TOOL) container ls -a |  grep ago | awk '{print $$1}' | xargs $(BUILD_TOOL) stop
	$(BUILD_TOOL) container ls -a |  grep ago | awk '{print $$1}' | xargs $(BUILD_TOOL) rm
	$(BUILD_TOOL) images -a | grep -v python | grep ago | awk '{print $$3}' | xargs $(BUILD_TOOL) rmi -f

build:
	$(BUILD_TOOL) build -f ./deployments/docker/Dockerfile --build-arg ENVIRONMENT=$e .

list:
	$(BUILD_TOOL) images

run:
	$(BUILD_TOOL) run $i

exec:
	$(BUILD_TOOL) run -it $i /bin/sh
