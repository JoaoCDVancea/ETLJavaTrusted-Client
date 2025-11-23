import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

public class S3 {
    private static final String BUCKET_TRUSTED = "s3-trusted-ontracksystems";
    private static final String BUCKET_CLIENT = "s3-client-ontracksystems";
    private static final Region REGIAO = Region.US_EAST_1;
    private static final ZoneId FUSO_BRASIL = ZoneId.of("America/Sao_Paulo");

    private static final S3Client s3 = S3Client.builder()
            .region(REGIAO)
            .build();

    private List<String> listarGaragens(S3Client s3) {
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(BUCKET_TRUSTED)
                .delimiter("/")
                .build();
        return s3.listObjectsV2(request).commonPrefixes().stream()
                .map(CommonPrefix::prefix)
                .collect(Collectors.toList());
    }


}
