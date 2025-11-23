import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class Main implements RequestHandler<Object, String> {
    private static final String BUCKET_TRUSTED = "s3-trusted-ontracksystems";
    private static final String BUCKET_CLIENT = "s3-client-ontracksystems";
    private static final Region REGIAO = Region.US_EAST_1;



    @Override
    public String handleRequest(Object input, Context context) {


        return "Sucesso!";
    }
}
