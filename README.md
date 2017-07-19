# Storm-CustomScheduler

For a DAG it does the mapping from threads to the VMs. The input to the scheduler is the mapping(in fixed format) in JSON file. " Rest Assured :-) " 

### Prerequisites

Clone the Apache Storm from the latest repos and save it to the head node where Nimbus is to be run. 

## Getting Started
Download the code and update the code with path for directory having Storm installation in file SiteAwareSchedulerwithJsonWithStateMatrixV1WithAcker.java (for DAG without Ackers) and SiteAwareSchedulerwithJsonWithStateMatrixV1.java (for DAG without Ackers).
```
String jsonfilepath = "<your_path>/apache-storm-1.0.1/conf/inputTopoConfig.json";
```



### Installing



1- Compile with the command:

```
  mvn clean install -DskipTests=true
```

2- Copy the jar file with suffix "jar-with-dependencies.jar" to lib folder of the Storm installation at head node (running Nimbus) only.

3- Update the storm.yaml with entries -
For DAG with Acker's
```
 storm.scheduler: "main.java.SiteAwareSchedulerwithJsonWithStateMatrixV1WithAcker"
 ```
 and for DAG without Acker's
 
 ```
 storm.scheduler: "main.java.SiteAwareSchedulerwithJsonWithStateMatrixV1"
```

```
 supervisor.slots.ports:
    - 6701
    - 6702
    - 6703
    - 6704

 supervisor.scheduler.meta:
     site: "orion0"
```

4- Put JSON file with mapping from Executors to VMs inside Storm conf folder.(name of json file must be exactly same as inputTopoConfig.json)

#### Sample JSON: 

Below is the json with the for the 2 DAGs with toponames - **[FullTopologyWithFiveBolt, FooLinearParseTopology].**

Each of them have the boltnames within them and **"conf"** corresponding to the entries of how threads of that bolt are to be mapped **("VM-Name1,ThreadCount1/VM-Name2,ThreadCount2")** and **"threads"** represents the total number of **threads (ThreadCount1+ThreadCount2) or Parallelism Hint** corresponding to that bolt.

 ```
 {
	"FullTopologyWithFiveBolt": {
		"identityblob": {
			"conf": "orion2#6709,50/orion2#6712,30",
			"threads": "80"
		},
		"identityparse": {
			"conf": "orion2#6712,1",
			"threads": "1"
		},
		"identitytable": {
			"conf": "orion2#6711,60/orion2#6712,10",
			"threads": "70"
		},
		"identitypi": {
			"conf": "orion2#6710,1/orion6#6741,1/orion2#6712,1",
			"threads": "3"
		},
		"batchFileWriteTaskBolt": {
			"conf": "orion2#6712,1",
			"threads": "1"
		},
		"sink": {
			"conf": "orion0#6701,1",
			"threads": "1"
		},
		"spout": {
			"conf": "orion0#6702,1",
			"threads": "1"
		}
	},
	
	"FooLinearParseTopology": {
		"fooPartial2": {
			"conf": "orion10#6761,1",
			"threads": "1"
		},
		"fooPartial3": {
			"conf": "orion10#6762,1",
			"threads": "1"
		},
		"fooPartial4": {
			"conf": "orion10#6763,1",
			"threads": "1"
		},
		"fooPartial5": {
			"conf": "orion10#6764,1",
			"threads": "1"
		},
		"fooPartial6": {
			"conf": "orion11#6765,1",
			"threads": "1"
		},
		"sink": {
			"conf": "orion0#6701,1",
			"threads": "1"
		},
		"spout": {
			"conf": "orion0#6702,1",
			"threads": "1"
		},
		"__acker": {
			"conf": "orion0#6703,1",
			"threads": "1"
		}
	}
}
```


## Deployment: In case Nimbus Kills itself then Check for the following issues

1- Make Sure that JSON file name with mapping must be **inputTopoConfig.json** and **placed in conf** folder of your Storm installation.

2- Names of Topo and bolt should be same as in JSON file.

3- Make sure that all supervisors have the different name as entry in     **site: "orionXX"** and entry for that Supervsor slot does exists for that host or site.

4- **Number of Threads** for every bolt in topo code should be same as in JSON file for the DAG to be submitted.

5- Make sure to use the correct scheduler option in case you are using the **scheduler with Ackers or without Ackers.** 

6- Check for **log in nimbus.log file** inside head node logs folder.

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management


## Author

* **Anshu Shukla** [, DreamLab-IISC](https://dream-lab.cds.iisc.ac.in/)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Thanks to my batchmates @Dream-Lab IISC


