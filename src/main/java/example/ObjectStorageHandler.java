package example;

import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.Item;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class ObjectStorageHandler {
    private  MinioClient minioClient;

    public ObjectStorageHandler() {

        this.minioClient = MinioClient.builder().endpoint("http://127.0.0.1", 9000, false)
                .credentials("minio", "minio123").build();

        /*this.minioClient =
                MinioClient.builder()
                        .endpoint("https://play.min.io")
                        .credentials("Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
                        .build();*/
    }

    public boolean createBucketIfNotExists(String bucketName) {

        try {
            if (!this.minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                System.out.println(bucketName + " created successfully");
            }

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;

    }

    public boolean uploadObjectFromPath(String bucketName, String objectName, String path) {

        try {
            this.minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .filename(path)
                            .build());

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public String getFileEmbedUrl(String bucketName, String objectName) {

        try {
            String url = minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(bucketName)
                            .object(objectName)
                            .expiry(60 * 60 * 24)
                            .build());
            return url;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public List<String> getUserDocumentUrls(String bucketName, String userNamePrefix) {
        Iterable<Result<Item>> results =
                this.minioClient.listObjects(ListObjectsArgs.builder()
                        .bucket(bucketName)
                        .prefix(userNamePrefix).recursive(true).build()
                );

        List<String> fileList = new ArrayList<>();

        for (Result<Item> result : results) {
            Item item = null;
            try {
                item = result.get();
                String fileUrl = this.getFileEmbedUrl(bucketName, item.objectName());
                fileList.add(fileUrl);
                System.out.println(":" + item.size() + "\t" + item.objectName());

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return fileList;
    }

    public static void main(String[] args)
            throws IOException, NoSuchAlgorithmException, InvalidKeyException {
        /* play.min.io for test and development. */

        ObjectStorageHandler mc = new ObjectStorageHandler();
        //Storage Hierarchy A-Z
        // Organise the buckets hierarchically for better performance
        // a000
        //   auniqueUserId-1
        //      auniqueUserDocument
        //   auniqueUserId-2
        //b000
        //   buniqueUserId-1
        //     auniqueUserDocument

        String aUserName = "AUser";
        String userUniqueId="id01";

        //Just comply with the bucket name: min 3 chars. (a000, b000, c000..)
        String bucketNameFromUserName = aUserName.substring(0,1).toLowerCase()+"000";

        String objectName = userUniqueId+ "/" + "WechatIMG20711113.jpeg";
        String filePath = "/home/prakash/Downloads/Temp/3MB.jpg";

        mc.createBucketIfNotExists(bucketNameFromUserName);
        mc.uploadObjectFromPath(bucketNameFromUserName, objectName,filePath);

        //Get a file Url for example
        String fileUrl = mc.getFileEmbedUrl(bucketNameFromUserName, objectName);
        System.out.println("Individual File URl:"+fileUrl);

        //Get all file urls of the user for example
        List<String> userFiles = mc.getUserDocumentUrls(bucketNameFromUserName, userUniqueId);
        for (String fUrl : userFiles) {
            System.out.println(":List File Url:" + fUrl);
        }


    }
}
