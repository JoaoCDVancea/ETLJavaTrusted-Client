import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class S3 implements RequestHandler<Object, String> {

    private static final String BUCKET_TRUSTED = System.getenv().getOrDefault("BUCKET_TRUSTED", "s3-trusted-ontracksystems");
    private static final String BUCKET_CLIENT = System.getenv().getOrDefault("BUCKET_CLIENT", "s3-client-ontracksystems");
    private static final Region REGIAO = Region.US_EAST_1;
    private static final ZoneId FUSO_BRASIL = ZoneId.of("America/Sao_Paulo");

    private static final S3Client s3 = S3Client.builder().region(REGIAO).build();
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Iniciando ETL Trusted -> Client (JSON Dashboard)...");

        try {
            ZonedDateTime agora = ZonedDateTime.now(FUSO_BRASIL);
            LocalDate dataHoje = agora.toLocalDate();

            context.getLogger().log("Processando dados para a data: " + dataHoje);

            List<String> garagens = listarGaragens(s3);

            for (String garagemPrefix : garagens) {
                processarGaragem(s3, garagemPrefix, dataHoje, agora, context);
            }

            return "Sucesso! Dashboard atualizada.";

        } catch (Exception e) {
            context.getLogger().log("ERRO CRITICO: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void processarGaragem(S3Client s3, String garagemPrefix, LocalDate dataHoje, ZonedDateTime agora, Context context) throws IOException {
        String garagemIdLimpo = garagemPrefix.replace("/", "").replace("idGaragem=", "");
        String nomeArquivoJson = "dashboard_" + garagemIdLimpo + ".json";

        Path jsonLocalPath = Paths.get("/tmp", nomeArquivoJson);

        DashboardData dashboardData = baixarOuCriarJson(s3, garagemPrefix + nomeArquivoJson, jsonLocalPath, garagemIdLimpo);
        dashboardData.lastUpdate = agora;

        String caminhoDiaTrusted = String.format("ano=%d/mes=%02d/dia=%02d/",
                dataHoje.getYear(), dataHoje.getMonthValue(), dataHoje.getDayOfMonth());

        String prefixoCompleto = garagemPrefix + caminhoDiaTrusted;
        List<Path> csvsDoDia = baixarCSVsDoDia(s3, prefixoCompleto, garagemPrefix);

        if (!csvsDoDia.isEmpty()) {
            atualizarEstatisticasDoDia(dashboardData, csvsDoDia, dataHoje);
        } else {
            context.getLogger().log("   [INFO] Sem dados novos hoje para " + garagemIdLimpo);
        }

        calcularResumosMensais(dashboardData, dataHoje);

        objectMapper.writeValue(jsonLocalPath.toFile(), dashboardData);
        uploadParaS3(s3, garagemPrefix + nomeArquivoJson, jsonLocalPath);

        context.getLogger().log("   [OK] JSON Atualizado: " + garagemPrefix + nomeArquivoJson);

        Files.deleteIfExists(jsonLocalPath);
        limparTemp(Paths.get("/tmp", garagemPrefix));
    }

    private void atualizarEstatisticasDoDia(DashboardData data, List<Path> csvs, LocalDate dataHoje) {
        DailyStats statsHoje = new DailyStats();
        CurrentStatus ultimoStatus = new CurrentStatus();

        double somaCpu = 0, somaRam = 0, somaDisk = 0;
        int totalPontos = 0;

        long minBytesUsados = Long.MAX_VALUE;
        long maxBytesUsados = Long.MIN_VALUE;

        csvs.sort(Comparator.comparing(Path::getFileName));

        for (Path csvPath : csvs) {
            try (CSVReader reader = new CSVReader(new FileReader(csvPath.toFile()))) {
                List<String[]> linhas = reader.readAll();
                if (linhas.isEmpty() || linhas.size() < 2) continue;

                for (int i = 1; i < linhas.size(); i++) {
                    String[] l = linhas.get(i);
                    try {
                        String timestamp = l[0];
                        double cpu = Double.parseDouble(l[2]);
                        double ram = Double.parseDouble(l[4]);
                        long diskTotalBytes = Long.parseLong(l[5]);
                        double diskPct = Double.parseDouble(l[6]);

                        long diskUsedBytes = (long) (diskTotalBytes * (diskPct / 100.0));

                        if (diskUsedBytes < minBytesUsados) minBytesUsados = diskUsedBytes;
                        if (diskUsedBytes > maxBytesUsados) maxBytesUsados = diskUsedBytes;

                        somaCpu += cpu;
                        somaRam += ram;
                        somaDisk += diskPct;
                        totalPontos++;

                        ultimoStatus.cpuPercent = cpu;
                        ultimoStatus.ramPercent = ram;
                        ultimoStatus.diskPercent = diskPct;
                        ultimoStatus.totalStorageGb = bytesParaGb(diskTotalBytes);
                        ultimoStatus.usedStorageGb = bytesParaGb(diskUsedBytes);
                        ultimoStatus.timestampStr = timestamp;

                    } catch (Exception e) { }
                }
            } catch (Exception e) { }
        }

        if (totalPontos > 0) {
            statsHoje.avgCpuPercent = arredondar(somaCpu / totalPontos, 2);
            statsHoje.avgRamPercent = arredondar(somaRam / totalPontos, 2);
            statsHoje.avgDiskPercent = arredondar(somaDisk / totalPontos, 2);
            statsHoje.dataPointsCount = totalPontos;

            long deltaBytes = Math.max(0, maxBytesUsados - minBytesUsados);
            statsHoje.dailyIngestedGb = bytesParaGb(deltaBytes);

            data.currentStatus = ultimoStatus;
            data.history.put(dataHoje, statsHoje);
        }
    }

    private void calcularResumosMensais(DashboardData data, LocalDate hoje) {
        data.currentMonthSummary = gerarResumoMensal(data, hoje, true);

        LocalDate mesPassado = hoje.minusMonths(1);
        data.lastMonthSummary = gerarResumoMensal(data, mesPassado, false);
    }

    private MonthlySummary gerarResumoMensal(DashboardData data, LocalDate dataReferencia, boolean isMesAtual) {
        MonthlySummary resumo = new MonthlySummary();
        resumo.monthName = dataReferencia.getMonth().toString();

        double totalGb = data.history.entrySet().stream()
                .filter(e -> e.getKey().getYear() == dataReferencia.getYear() &&
                        e.getKey().getMonth() == dataReferencia.getMonth())
                .mapToDouble(e -> e.getValue().dailyIngestedGb)
                .sum();

        resumo.totalIngestedGb = arredondar(totalGb, 2);

        if (isMesAtual) {
            int diaAtual = dataReferencia.getDayOfMonth();
            int diasNoMes = dataReferencia.lengthOfMonth();
            if (diaAtual > 0) {
                double projecao = (totalGb / diaAtual) * diasNoMes;
                resumo.projectionGb = arredondar(projecao, 2);
            } else {
                resumo.projectionGb = 0.0;
            }
        } else {
            resumo.projectionGb = totalGb;
        }
        return resumo;
    }

    private DashboardData baixarOuCriarJson(S3Client s3, String key, Path destino, String garageId) {
        try {
            Files.deleteIfExists(destino);
            GetObjectRequest req = GetObjectRequest.builder().bucket(BUCKET_CLIENT).key(key).build();
            s3.getObject(req, destino);
            return objectMapper.readValue(destino.toFile(), DashboardData.class);
        } catch (NoSuchKeyException e) {
            DashboardData novo = new DashboardData();
            novo.garageId = garageId;
            return novo;
        } catch (Exception e) {
            System.err.println("Erro ao ler JSON: " + e.getMessage());
            DashboardData novo = new DashboardData();
            novo.garageId = garageId;
            return novo;
        }
    }

    private List<Path> baixarCSVsDoDia(S3Client s3, String prefixo, String garagemFolder) throws IOException {
        List<Path> arquivos = new ArrayList<>();
        Path tempDir = Paths.get("/tmp", garagemFolder, "trusted_day");
        if (!Files.exists(tempDir)) Files.createDirectories(tempDir);

        ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(BUCKET_TRUSTED).prefix(prefixo).build();
        List<S3Object> objs = s3.listObjectsV2(req).contents();

        for (S3Object obj : objs) {
            if (!obj.key().endsWith(".csv")) continue;
            Path destino = tempDir.resolve(Paths.get(obj.key()).getFileName());
            try {
                Files.deleteIfExists(destino);
                s3.getObject(GetObjectRequest.builder().bucket(BUCKET_TRUSTED).key(obj.key()).build(), destino);
                arquivos.add(destino);
            } catch (Exception e) { }
        }
        return arquivos;
    }

    private void uploadParaS3(S3Client s3, String key, Path localPath) {
        PutObjectRequest req = PutObjectRequest.builder().bucket(BUCKET_CLIENT).key(key).build();
        s3.putObject(req, RequestBody.fromFile(localPath));
    }

    private List<String> listarGaragens(S3Client s3) {
        ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(BUCKET_TRUSTED).delimiter("/").build();
        return s3.listObjectsV2(req).commonPrefixes().stream().map(CommonPrefix::prefix).collect(Collectors.toList());
    }

    private void limparTemp(Path path) {
        try {
            if (Files.exists(path)) {
                Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
            }
        } catch (Exception e) { }
    }

    private double bytesParaGb(long bytes) {
        if (bytes <= 0) return 0.0;
        return arredondar(bytes / (1024.0 * 1024.0 * 1024.0), 2);
    }

    private double arredondar(double valor, int casas) {
        if (Double.isNaN(valor) || Double.isInfinite(valor)) return 0.0;
        long fator = (long) Math.pow(10, casas);
        return (double) Math.round(valor * fator) / fator;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DashboardData {
        public String garageId;
        public ZonedDateTime lastUpdate;
        public CurrentStatus currentStatus = new CurrentStatus();
        public MonthlySummary currentMonthSummary = new MonthlySummary();
        public MonthlySummary lastMonthSummary = new MonthlySummary();
        public Map<LocalDate, DailyStats> history = new HashMap<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CurrentStatus {
        public double cpuPercent;
        public double ramPercent;
        public double diskPercent;
        public double totalStorageGb;
        public double usedStorageGb;
        public String timestampStr;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DailyStats {
        public double avgCpuPercent;
        public double avgRamPercent;
        public double avgDiskPercent;
        public double dailyIngestedGb;
        public int dataPointsCount;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MonthlySummary {
        public String monthName;
        public double totalIngestedGb;
        public double projectionGb;
    }
}