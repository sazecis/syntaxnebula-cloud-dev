package com.syntaxnebula.aws;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;
import software.amazon.awssdk.services.s3.model.TopicConfiguration;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import java.util.Arrays;

public class S3Demo {
    private static S3Client s3;

    public static void main(String[] args) {
        if (args.length == 0 || "help".equals(args[0])) {
            System.out.println("Usage: java -jar S3Demo-0.1.jar <command> [<args>]");
            System.out.println("Commands:");
            System.out.println("  create-bucket <bucket-name> - Creates a new S3 bucket with the specified name.");
            System.out.println("  upload-object <bucket-name> <file-path> - Uploads a file to the specified S3 bucket.");
            System.out.println("  download-object <bucket-name> <object-key> - Downloads an object from the specified S3 bucket.");
            System.out.println("  delete-bucket <bucket-name> - Deletes the specified S3 bucket (must be empty).");
            System.out.println("  create-website <bucket-name> - Configures the specified S3 bucket for static website hosting.");
            System.out.println("  add-notification <bucket-name> <topic-arn> - Configures the specified S3 bucket to send notification when a new object is uploaded.");
            System.out.println();
            System.out.println("Options:");
            System.out.println("  help - Shows this help message.");
            System.out.println();
            System.out.println("Examples:");
            System.out.println("  java -jar S3Demo-0.1.jar create-bucket my-awesome-bucket");
            System.out.println("  java -jar S3Demo-0.1.jar upload-object my-awesome-bucket ./path/to/myfile.txt");
            System.out.println("  java -jar S3Demo-0.1.jar download-object my-awesome-bucket myfile.txt");
            System.out.println("  java -jar S3Demo-0.1.jar delete-bucket my-awesome-bucket");
            System.out.println("  java -jar S3Demo-0.1.jar create-website my-awesome-bucket");
            System.exit(1);
        }

        Region region = Region.of(new DefaultAwsRegionProviderChain().getRegion().id());
        s3 = S3Client.builder()
                     .region(region)
                     .build();

        try {
            switch (args[0]) {
                case "create-bucket":
                    if (args.length < 2) {
                        System.out.println("Bucket name required");
                        System.exit(1);
                    }
                    createBucket(args[1]);
                    break;
                case "upload-object":
                    if (args.length < 3) {
                        System.out.println("Bucket name and file path required");
                        System.exit(1);
                    }
                    uploadObject(args[1], args[2]);
                    break;
                case "download-object":
                    if (args.length < 3) {
                        System.out.println("Bucket name and object key required");
                        System.exit(1);
                    }
                    downloadObject(args[1], args[2]);
                    break;
                case "delete-bucket":
                    if (args.length < 2) {
                        System.out.println("Bucket name required");
                        System.exit(1);
                    }
                    deleteBucket(args[1]);
                    break;
                case "create-website":
                    if (args.length <2) {
                        System.out.println("Bucket name required");
                        System.exit(1);
                    }
                    createStaticWebsite(args[1]);
                    break;
                case "add-notification":
                    if (args.length <3) {
                        System.out.println("Bucket name and topic ARN is required");
                        System.exit(1);
                    }
                    addNotification(args[1], args[2]);
                    break;
                default:
                    System.out.println("Invalid command");
                    System.exit(1);
            }
        } finally {
            s3.close();
        }
    }

    private static void createBucket(String bucketName) {
        if (!bucketExists(bucketName)) {
            try {
                s3.createBucket(CreateBucketRequest
                        .builder()
                        .bucket(bucketName)
                        .build());
                System.out.println("Creating bucket: " + bucketName);
                s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                        .bucket(bucketName)
                        .build());
                System.out.println(bucketName + " is ready.");
                System.out.printf("%n");
            } catch (S3Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        } else {
            System.out.println("Bucket already exists: " + bucketName);
        }
    }
        
    private static boolean bucketExists(String bucketName) {
        try {
            s3.headBucket(HeadBucketRequest.builder()
                           .bucket(bucketName)
                           .build());
            System.out.println("Bucket '" + bucketName + "' exists and you have permission to access it.");
            return true;  // Bucket exists and you have permission to access it
        } 
        catch (AwsServiceException awsEx) {
            switch (awsEx.statusCode()) {
                case 404:
                    System.out.println("No such bucket exists.");
                    return false;
                case 400:
                    System.out.println("Attempted to access a bucket from a Region other than where it exists.");
                    return true;
                case 403:
                    System.out.println("Permission errors in accessing bucket...");
                    return true;
            }
        }
        return true;

    }

    private static void uploadObject(String bucketName, String filePath) {
        if (bucketExists(bucketName)) {
            Path file = Paths.get(filePath);
            String key = file.getFileName().toString();

            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                         RequestBody.fromFile(file));
            System.out.println("Object uploaded: " + key + " to " + bucketName);
        } else {
            System.out.println("Bucket does not exist: " + bucketName);
        }
    }

    private static void downloadObject(String bucketName, String key) {
        if (bucketExists(bucketName)) {
            Path filePath = Paths.get(key);

            try {
                if (Files.exists(filePath)) {
                    Files.delete(filePath);
                }
                GetObjectResponse getObjectResponse = s3.getObject(GetObjectRequest.builder()
                                                                 .bucket(bucketName)
                                                                 .key(key)
                                                                 .build(),
                    ResponseTransformer.toFile(filePath));
                System.out.println("Object downloaded: " + key + ", Content Length: " + getObjectResponse.contentLength());
            } catch (IOException ex) {
                System.err.println("Error during file operation: " + ex.getMessage());
                System.exit(1);
            }
        } else {
            System.out.println("Bucket does not exist: " + bucketName);
        }
    }
    
    private static void createStaticWebsite(String bucketName) {
        String indexDocument = "index.html";
        String errorDocument = "error.html";
        try {
            // Remove block public access settings
            s3.putPublicAccessBlock(PutPublicAccessBlockRequest.builder()
                .bucket(bucketName)
                .publicAccessBlockConfiguration(PublicAccessBlockConfiguration.builder()
                    .blockPublicAcls(false)
                    .ignorePublicAcls(false)
                    .blockPublicPolicy(false)
                    .restrictPublicBuckets(false)
                    .build())
                .build());
    
            System.out.println("Public access block settings are turned off for the bucket.");
            
            // Define bucket policy directly in the code
            String bucketPolicy = "{\n" +
                "   \"Version\":\"2012-10-17\",\n" +
                "   \"Statement\":[\n" +
                "       {\n" +
                "           \"Sid\":\"PublicReadGetObject\",\n" +
                "           \"Effect\":\"Allow\",\n" +
                "           \"Principal\": \"*\",\n" +
                "           \"Action\": \"s3:GetObject\",\n" +
                "           \"Resource\": \"arn:aws:s3:::" + bucketName + "/*\"\n" +
                "       }\n" +
                "   ]\n" +
                "}";
    
            // Set the bucket policy to make the bucket public
            s3.putBucketPolicy(PutBucketPolicyRequest.builder()
                                 .bucket(bucketName)
                                 .policy(bucketPolicy)
                                 .build());
    
            System.out.println("Bucket policy set to public.");
    
            // Configure the bucket for static website hosting
            s3.putBucketWebsite(PutBucketWebsiteRequest.builder()
                                 .bucket(bucketName)
                                 .websiteConfiguration(WebsiteConfiguration.builder()
                                                        .indexDocument(IndexDocument.builder()
                                                                        .suffix(indexDocument)
                                                                        .build())
                                                        .errorDocument(ErrorDocument.builder()
                                                                        .key(errorDocument)
                                                                        .build())
                                                        .build())
                                 .build());
    
            System.out.println("Static website hosting is enabled for the bucket.");
        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        } finally {
            s3.close();
        }
    }
    
    private static void deleteBucket(String bucketName) {
        try {
            s3.deleteBucket(DeleteBucketRequest.builder()
                               .bucket(bucketName)
                               .build());
            System.out.println("Bucket deleted: " + bucketName);
        } catch (S3Exception ex) {
            System.err.println("Error deleting bucket: " + ex.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private static void addNotification(String bucketName, String topicArn) {
        try {
            TopicConfiguration topicConfig = TopicConfiguration.builder()
                .id("NewObjectCreationNotification") 
                .events(Arrays.asList(Event.S3_OBJECT_CREATED)) 
                .topicArn(topicArn) 
                .build();
    
            NotificationConfiguration notificationConfiguration = NotificationConfiguration.builder()
                .topicConfigurations(topicConfig)
                .build();
    
            PutBucketNotificationConfigurationRequest request = PutBucketNotificationConfigurationRequest.builder()
                .bucket(bucketName)
                .notificationConfiguration(notificationConfiguration)
                .build();
            
            s3.putBucketNotificationConfiguration(request);
            System.out.println("Notification configuration added successfully for bucket: " + bucketName);
        } catch (S3Exception e) {
            System.err.println("Failed to add notification configuration: " + e.awsErrorDetails().errorMessage());
        }
    }
    
}
