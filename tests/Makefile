

setup: checkout-testdata docker-compose-up

# checkout-testdata will checkout testdata from separate repository into git submodule.
checkout-testdata:
	git submodule init
	git submodule update
	cd ./testdata && git fetch && git checkout master && git pull

# docker-compose-up will download docker image for each of the databases listed in docker-compose.yaml file, and then it will initialize
# database with testdata retrieved in previous step.
# On the first run this action might take couple of minutes. Docker temp data are stored in .docker directory.
docker-compose-up:
	docker-compose up

init-all:
	go run ./init/init.go -testsuite all

init-postgres:
	go run ./init/init.go -testsuite postgres

init-mysql:
	go run ./init/init.go -testsuite mysql

init-mariadb:
	go run ./init/init.go -testsuite mariadb

init-sqlite:
	go run ./init/init.go -testsuite sqlite

# jet-gen will call generator on each of the test databases to generate sql builder and model files need to run the tests.
jet-gen-all: install-jet-gen jet-gen-postgres jet-gen-mysql jet-gen-mariadb jet-gen-sqlite

ifeq ($(OS),Windows_NT)
    target := jet.exe
else
    target := jet
endif

install-jet-gen:
	go build -o ${GOPATH}/bin/${target} ../cmd/jet/

jet-gen-postgres:
	jet -dsn=postgres://jet:jet@localhost:50901/jetdb?sslmode=disable -schema=dvds  -path=./.gentestdata/
	jet -dsn=postgres://jet:jet@localhost:50901/jetdb?sslmode=disable -schema=chinook  -path=./.gentestdata/
	jet -dsn=postgres://jet:jet@localhost:50901/jetdb?sslmode=disable -schema=chinook2  -path=./.gentestdata/
	jet -dsn=postgres://jet:jet@localhost:50901/jetdb?sslmode=disable -schema=northwind  -path=./.gentestdata/
	jet -dsn=postgres://jet:jet@localhost:50901/jetdb?sslmode=disable -schema=test_sample  -path=./.gentestdata/

jet-gen-mysql:
	jet -source=mysql -dsn="jet:jet@tcp(localhost:50902)/dvds" -path=./.gentestdata/mysql
	jet -source=mysql -dsn="jet:jet@tcp(localhost:50902)/dvds2" -path=./.gentestdata/mysql
	jet -source=mysql -dsn="jet:jet@tcp(localhost:50902)/test_sample" -path=./.gentestdata/mysql

jet-gen-mariadb:
	jet -source=mariadb -dsn="jet:jet@tcp(localhost:50903)/dvds" -path=./.gentestdata/mysql
	jet -source=mariadb -dsn="jet:jet@tcp(localhost:50903)/dvds2" -path=./.gentestdata/mysql
	jet -source=mariadb -dsn="jet:jet@tcp(localhost:50903)/test_sample" -path=./.gentestdata/mysql

jet-gen-sqlite:
	jet -source=sqlite -dsn="./testdata/init/sqlite/chinook.db" -schema=dvds -path=./.gentestdata/sqlite/chinook
	jet -source=sqlite -dsn="./testdata/init/sqlite/sakila.db" -schema=dvds -path=./.gentestdata/sqlite/sakila
	jet -source=sqlite -dsn="./testdata/init/sqlite/test_sample.db" -schema=dvds -path=./.gentestdata/sqlite/test_sample

jet-gen-cockroach:
	jet -dsn=postgres://jet:jet@127.0.0.1:26257/jetdb?sslmode=disable -schema=dvds        -path=./.gentestdata/
	jet -dsn=postgres://jet:jet@localhost:26257/jetdb?sslmode=disable -schema=chinook     -path=./.gentestdata/
	jet -dsn=postgres://jet:jet@localhost:26257/jetdb?sslmode=disable -schema=chinook2    -path=./.gentestdata/
	jet -dsn=postgres://jet:jet@localhost:26257/jetdb?sslmode=disable -schema=northwind   -path=./.gentestdata/
	jet -dsn=postgres://jet:jet@localhost:26257/jetdb?sslmode=disable -schema=test_sample -path=./.gentestdata/

# docker-compose-cleanup will stop and remove test containers, volumes, and images.
cleanup:
	docker-compose down --volumes
