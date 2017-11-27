CXX=spark-submit
CXXFLAGS=--num-executors=8 --executor-cores 4
input = input.txt
output = output

all:
	$(CXX) $(CXXFLAGS) cc.scala 

demo:
	$(CXX) $(CXXFLAGS) ex05.py $(input) $(output)

clean:
	rm -r output
