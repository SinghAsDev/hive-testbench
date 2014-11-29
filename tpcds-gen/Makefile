
all: target/lib/dsdgen.jar target/tpcds-gen-1.0-SNAPSHOT.jar

target/tpcds-gen-1.0-SNAPSHOT.jar: $(shell find -name *.java)
	mvn package

target/tpcds_kit: tpcds_kit
	mkdir -p target/
	rm -rf target/tpcds-kit
	mv tpcds-kit target/

tpcds_kit:
	git clone "https://github.com/grahn/tpcds-kit.git"

target/lib/dsdgen.jar: target/tools/dsdgen
	cd "target/tpcds-kit"; mkdir -p ../lib/; ( jar cvf ../lib/dsdgen.jar tools/ || gjar cvf ../lib/dsdgen.jar tools/ )

target/tools/dsdgen: target/tpcds_kit
	cd "target/tpcds-kit/tools"; make clean; make dsdgen

clean:
	mvn clean
