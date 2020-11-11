# CS226 Assignment1

## Building the project
`$ mvn package`

## Use the hadoop command to execute the program
```sh
$ hadoop jar <jar_file_path> edu.ucr.cs.cs226.xkong016.HDFSUpload <source_file_path> <target_file_path>
```

*Valid File Path:*
1. `local file: file:/// + the absolute path of the local file`

2. `HDFS fileL: hdfs:// + the absolute path of the HDFS file`
