CXX=spark-submit
CXXFLAGS=--num-executors=8 --executor-cores 4
input = input.txt
output = output

all:
	#$(CXX) $(CXXFLAGS) ex05.py $(input) $(output)
	#################This is for scala##############
	sbt package
	$(CXX) $(CXXFLAGS) --class "a2" --master local[4] target/scala-2.11/a2_2.11-1.0.jar $(input) $(output)
	#################Scala ends here##############
demo:
	# $(CXX) $(CXXFLAGS) ex05.py $(input) $(output)
	#################This is for scala##############
	$(CXX) $(CXXFLAGS) --class "a2" --master local[4] target/scala-2.11/a2_2.11-1.0.jar $(input) $(output)
	#################Scala ends here##############
clean:#use this for deleting the output folder
	rm -r output
clean-all:
	rm -r output
	#################This is for scala##############
	rm -r project
	rm -r target
	#################Scala ends here##############
