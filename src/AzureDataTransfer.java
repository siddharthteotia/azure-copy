import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.CommonRestResponse;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TransferManager;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpClientConfiguration;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AzureDataTransfer {
  private final ContainerURL _azureStorageContainerURL;
  private final String _pathToAvroFilesDirectory;
  private final ThreadPoolExecutor _workerPool;
  private final ExecutorCompletionService<UploadResult> _completionService;

  private AzureDataTransfer(
      AzureStorageAccountInfo azureStorageAccountInfo,
      final String pathToAvroFilesDirectory,
      final String proxyHostName,
      final int proxyPort,
      final int threadPoolSize
      ) throws Exception {

    final SharedKeyCredentials sharedKeyCredentials;

    try {
      sharedKeyCredentials = new SharedKeyCredentials(azureStorageAccountInfo._azureStorageAccountName, azureStorageAccountInfo._azureStorageAccountKey);
    } catch (InvalidKeyException e) {
      System.out.println("Invalid account storage account key provided");
      throw e;
    }

    System.out.println("Successfully created the credentials to access the storage account");

    final Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHostName, proxyPort));
    final HttpClient httpClient = HttpClient.createDefault(new HttpClientConfiguration(proxy));

    System.out.println("Using HTTP Proxy: " + proxy.toString());

    final PipelineOptions pipelineOptions = new PipelineOptions().withClient(httpClient);
    final HttpPipeline httpPipeline = StorageURL.createPipeline(sharedKeyCredentials, pipelineOptions);
    final URL accountURL;
    final ServiceURL serviceURL;

    try {
      accountURL = new URL(azureStorageAccountInfo._azureStorageAccountURL);
      serviceURL = new ServiceURL(accountURL, httpPipeline);
    } catch (Exception e) {
      System.out.println("ERROR: Failed to create azure storage account endpoint from URL");
      throw e;
    }

    System.out.println("Successfully create the account URL and service URL");

    try {
      _azureStorageContainerURL = serviceURL.createContainerURL(azureStorageAccountInfo._azureStorageContainer);
      _azureStorageContainerURL.create().blockingGet();
    } catch (Exception e) {
      System.out.println("ERROR: Failed to create container");
      throw e;
    }

    System.out.println("Successfully created container");

    _pathToAvroFilesDirectory = pathToAvroFilesDirectory;

    int numCores = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
    int numWorkerThreads = Math.min(numCores, Math.max(threadPoolSize, 1));
    _workerPool = (ThreadPoolExecutor)Executors.newFixedThreadPool(numWorkerThreads);
    _completionService = new ExecutorCompletionService<>(_workerPool);

    System.out.println("Using worker pool of " +  numWorkerThreads +  " threads");
  }

  public void upload() throws Exception {
    File avroFilesDirectory = new File(_pathToAvroFilesDirectory);
    String[] avroFiles = avroFilesDirectory.list();
    List<Future<UploadResult>> futures = new ArrayList<>();

    for (String avroFileName : avroFiles) {
      String pathToAvroFile = _pathToAvroFilesDirectory + "/" + avroFileName;
      File avroFile = new File(pathToAvroFile);
      UploadTask uploadTask = new UploadTask(avroFileName, avroFile, _azureStorageContainerURL);
      futures.add(_completionService.submit(uploadTask));
    }

    for (Future<UploadResult> future : futures) {
      UploadResult uploadResult;
      try {
        uploadResult = future.get();
        System.out.println(uploadResult.toString());
      } catch (Exception e) {
        System.out.println("Caught exception while waiting for future. Error: " +  e);
      }
    }

    try {
      _workerPool.awaitTermination(1, TimeUnit.HOURS);
    } catch (Exception e) {
      System.out.println("Caught exception while awaiting executor termination");
      throw e;
    } finally {
      _workerPool.shutdownNow();
    }
  }

  public static class UploadTask implements Callable<UploadResult> {
    private final String _fileName;
    private final File _fileToUpload;
    private final ContainerURL _azureStorageContainerURL;

    private UploadTask(String fileName, File file, ContainerURL azureStorageContainerURL) {
      _fileName = fileName;
      _fileToUpload = file;
      _azureStorageContainerURL = azureStorageContainerURL;
    }

    @Override
    public UploadResult call() {
      Single<CommonRestResponse> uploadResponse;
      try {
        AsynchronousFileChannel asyncChannel = AsynchronousFileChannel.open(_fileToUpload.toPath(), StandardOpenOption.READ);
        BlockBlobURL blockBlobURL = _azureStorageContainerURL.createBlockBlobURL(_fileName);
        uploadResponse  = TransferManager.uploadFileToBlockBlob(asyncChannel, blockBlobURL, BlockBlobURL.MAX_STAGE_BLOCK_BYTES, 100 *1024 *1024, null);
      } catch (Exception e) {
        System.out.println("Caught exception while uploading blob: " + _fileName + " error: " + e);
        return new UploadResult(_fileToUpload.getAbsolutePath(), null, e);
      }

      CommonRestResponse commonRestResponse;
      try {
        commonRestResponse = uploadResponse.blockingGet();
        System.out.println("Upload response status: " + commonRestResponse.statusCode()  + " response: " + commonRestResponse.response().toString());
      } catch (Exception e) {
        System.out.println("Caught exception while waiting to get upload response");
        return new UploadResult(_fileToUpload.getAbsolutePath(), null, e);
      }

      return new UploadResult(_fileToUpload.getAbsolutePath(), commonRestResponse, null);
    }
  }

  private static class UploadResult {
    private final String _file;
    private final CommonRestResponse _commonRestResponse;
    private final Exception _exception;

    private UploadResult(final String file, final CommonRestResponse commonRestResponse, final Exception exception) {
      _file = file;
      _commonRestResponse = commonRestResponse;
      _exception = exception;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("FileName: ").append(_file);
      if (_commonRestResponse != null) {
        sb.append(" Response status: ").append(_commonRestResponse.statusCode()).append(" response message: ").append(_commonRestResponse.response());
      }
      if (_exception != null) {
        sb.append(" exception: ").append(_exception.getCause()).append(" ").append(_exception.getMessage());
      }
      return sb.toString();
    }
  }

  private static class AzureStorageAccountInfo {
    private final String _azureStorageAccountName;
    private final String _azureStorageAccountURL;
    private final String _azureStorageAccountKey;
    private final String _azureStorageContainer;

    AzureStorageAccountInfo(
        final String azureStorageAccountName,
        final String azureStorageAccountURL,
        final String azureStorageAccountKey,
        final String azureStorageContainer) {
      _azureStorageAccountName = azureStorageAccountName;
      _azureStorageAccountURL = azureStorageAccountURL;
      _azureStorageAccountKey = azureStorageAccountKey;
      _azureStorageContainer = azureStorageContainer;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 7
        || args[0].equalsIgnoreCase("help")
        || args[0].equalsIgnoreCase("-h")
        || args[0].equalsIgnoreCase("-help")) {
      printUsage();
    }

    final String azureStorageAccountName = args[0];
    final String azureStorageAccountURL = args[1];
    final String azureStorageAccountKey = args[2];
    final String azureStorageContainer = args[3];
    final String pathToAvroFiles = args[4];
    final String proxyHostName = args[5];
    final int proxyPort = Integer.parseInt(args[6]);

    AzureStorageAccountInfo azureStorageAccountInfo =
        new AzureStorageAccountInfo(azureStorageAccountName, azureStorageAccountURL, azureStorageAccountKey, azureStorageContainer);

    AzureDataTransfer azureDataTransfer = new AzureDataTransfer(azureStorageAccountInfo, pathToAvroFiles, proxyHostName, proxyPort, 5);
    azureDataTransfer.upload();
  }

  private static void printUsage() {
    StringBuilder sb = new StringBuilder();
    sb.append("Following command line arguments are required\n\n")
        .append("Azure storage account name (STRING)\n")
        .append("Azure storage account url (STRING)\n")
        .append("Azure storage account key (STRING)\n")
        .append("Azure storage account container (STRING)\n")
        .append("Absolute path to directory containing gzipped Avro files (STRING)\n")
        .append("Proxy host name (STRING)\n")
        .append("Proxy port (INT)\n");
    System.out.println(sb.toString());
  }
}
