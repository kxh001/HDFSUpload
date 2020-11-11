package edu.ucr.cs.cs226.xkong016;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.*;

public class HDFSUpload
{
    public static void CopyFromLocalToLocal(File source_file, File target_file) throws FileNotFoundException{
        long start_time1 = System.currentTimeMillis();
        InputStream in1 = null;
        OutputStream out1 = null;
        try{
            in1 = new FileInputStream(source_file);
            out1 = new FileOutputStream(target_file);
            IOUtils.copyBytes(in1, out1, 4096,true);
        }catch (IOException e){
            System.out.println("File couldn't be copied");
            e.printStackTrace();
            System.exit(1);
        }
        finally {
            IOUtils.closeStream(in1);
            IOUtils.closeStream(out1);
        }
        long end_time1 = System.currentTimeMillis();
        double total_time1 = (end_time1 - start_time1);
        System.out.println("It takes "+total_time1+" ms to copy the file from local to the local.");
    }

    public static void CopyFromLocalToHDFS(File source_file, FileSystem target_fs, Path target_hdfs_path) throws IOException {
        long start_time2 = System.currentTimeMillis();
        InputStream in2 = null;
        OutputStream out2 = null;
        try{
            in2 = new FileInputStream(source_file);
            out2 = target_fs.create(target_hdfs_path);
            IOUtils.copyBytes(in2, out2, 4096,true);
        }catch (IOException e){
            System.out.println("File couldn't be copied");
            e.printStackTrace();
            System.exit(1);
        }
        finally {
            IOUtils.closeStream(in2);
            IOUtils.closeStream(out2);
        }
        long end_time2 = System.currentTimeMillis();
        double total_time2 = (end_time2 - start_time2);
        System.out.println("It takes "+total_time2+" ms to copy the file from local to HDFS.");
    }

    public static void CopyFromHDFSToLocal(FileSystem source_fs, Path source_hdfs_path, File target_file) throws IOException {
        long start_time3 = System.currentTimeMillis();
        InputStream in3 = source_fs.open(source_hdfs_path);
        OutputStream out3 = new FileOutputStream(target_file);
        try{
            IOUtils.copyBytes(in3, out3, 4096, true);
        }catch (IOException e){
            System.out.println("File couldn't be copied");
            e.printStackTrace();
            System.exit(1);
        }
        finally {
            IOUtils.closeStream(in3);
            IOUtils.closeStream(out3);
        }
        long end_time3 = System.currentTimeMillis();
        double total_time3 = (end_time3 - start_time3);
        System.out.println("It takes "+total_time3+" ms to copy the file from HDFS to local.");
    }

    public static void main(String args[]) throws IOException{
        if(args.length < 2){
            System.out.println("ERROR : Please enter the configurations " +
                    "of source_path and target_path");
            System.out.println("EXITING");
            System.exit(1);
        }
        String source_path_read = args[0];
        String target_path_read = args[1];
        String source_path = null;
        String target_path = null;
        URI uri = null;
        try{
            uri = new URI(source_path_read);
            source_path = uri.getPath();
            uri = new URI(target_path_read);
            target_path = uri.getPath();
        }catch(URISyntaxException e) {
            System.out.println("URI Exception =" + e.getMessage());
            System.exit(1);
        }
        File source_file;
        File target_file;
        Configuration source_config;
        Configuration target_config;
        Path source_hdfs_path;
        Path target_hdfs_path;
        FileSystem source_fs;
        FileSystem target_fs;


        // The source file is on local FS
        if(source_path_read.substring(0,4).equals("file")){
            //Check whether the source file exists
            source_file = new File(source_path);
            if(!source_file.exists()){
                System.out.println("ERROR : The source file does not exist");
                System.out.println("EXITING");
                System.exit(1);
            }
            // The target file is on local FS
            if(target_path_read.substring(0,4).equals("file")){
                //Check whether the target file exists
                target_file = new File(target_path);
                if(target_file.exists()){
                    System.out.println("ERROR : The target file already exists");
                    System.out.println("EXITING");
                    System.exit(1);
                }
                System.out.println("Copying file from local to local...");
                CopyFromLocalToLocal(source_file,target_file);
            }
            else{ // The target file is on HDFS
                //Check whether the target file exists
                target_config = new Configuration();
                target_hdfs_path = new Path(target_path);
                target_fs = FileSystem.get(target_config);
                if(target_fs.exists(target_hdfs_path)){
                    System.out.println("ERROR : The target file already exists");
                    System.out.println("EXITING");
                    System.exit(1);
                }
                System.out.println("Copying file from local to HDFS...");
                CopyFromLocalToHDFS(source_file,target_fs,target_hdfs_path);
            }
        }
        else{ // The source file is on HDFS
            if(target_path_read.substring(0,4).equals("file")) {
                //Check whether the source file exists
                source_config = new Configuration();
                source_hdfs_path = new Path(source_path);
                source_fs = FileSystem.get(source_config);
                if (!source_fs.exists(source_hdfs_path)) {
                    System.out.println("ERROR : The source file does not exist");
                    System.out.println("EXITING");
                    System.exit(1);
                }
                //Check whether the target file exists
                target_file = new File(target_path);
                if (target_file.exists()) {
                    System.out.println("ERROR : The target file already exists");
                    System.out.println("EXITING");
                    System.exit(1);
                }
                System.out.println("Copying file from HDFS to local...");
                CopyFromHDFSToLocal(source_fs,source_hdfs_path,target_file);
            }else{
                System.out.println("We don't copy file from HDFS to HDFS!");
            }
        }
    }
}
