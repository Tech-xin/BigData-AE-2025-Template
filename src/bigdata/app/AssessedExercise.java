package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import bigdata.objects.AssetRanking;
import bigdata.objects.StockPrice;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;
import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.pairing.AssetMetadataPairing;
import scala.Tuple2;

public class AssessedExercise {

	public static void main(String[] args) throws InterruptedException {

		// --------------------------------------------------------
		// Static Configuration
		// --------------------------------------------------------
		String datasetEndDate = "2020-04-01";
		double volatilityCeiling = 4;
		double peRatioThreshold = 25;

		long startTime = System.currentTimeMillis();

		// The code submitted for the assessed exerise may be run in either local or
		// remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef == null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can
															// get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that
																				// Spark finds it
			sparkMasterDef = "local[4]"; // default is local mode with two executors
		}

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf().setMaster(sparkMasterDef).setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// Get the location of the asset pricing data
		String pricesFile = System.getenv("BIGDATA_PRICES");
		if (pricesFile == null)
			pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries

		// Get the asset metadata
		String assetsFile = System.getenv("BIGDATA_ASSETS");
		if (assetsFile == null)
			assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries

		// ----------------------------------------
		// Pre-provided code for loading the data
		// ----------------------------------------

		// Create Datasets based on the input files

		// Load in the assets, this is a relatively small file
		Dataset<Row> assetRows = spark.read().option("multiLine", true).json(assetsFile);
		// assetRows.printSchema();
		System.err.println(assetRows.first().toString());
		JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());

		// Load in the prices, this is a large file (not so much in data size, but in
		// number of records)
		Dataset<Row> priceRows = spark.read().csv(pricesFile); // read CSV file
		Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
		Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert
																													// to
																													// Stock
																													// Price
																													// Objects

		AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling,
				peRatioThreshold);

		System.out.println(finalRanking.toString());

		System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");

		Thread.sleep(60000);

		// Close the spark session
		spark.close();

		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out != null)
			resultsDIR = out;

		long endTime = System.currentTimeMillis();

		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(resultsDIR).getAbsolutePath() + "/SPARK.DONE")));

			Instant sinstant = Instant.ofEpochSecond(startTime / 1000);
			Date sdate = Date.from(sinstant);

			Instant einstant = Instant.ofEpochSecond(endTime / 1000);
			Date edate = Date.from(einstant);

			writer.write("StartTime:" + sdate.toGMTString() + '\n');
			writer.write("EndTime:" + edate.toGMTString() + '\n');
			writer.write("Seconds: " + ((endTime - startTime) / 1000) + '\n');
			writer.write('\n');
			writer.write(finalRanking.toString());
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static AssetRanking rankInvestments(SparkSession spark, JavaPairRDD<String, AssetMetadata> assetMetadata,
			Dataset<StockPrice> prices, String datasetEndDate, double volatilityCeiling, double peRatioThreshold) {
		//-----------------------------------------
		// Student Solution Here
		//-----------------------------------------
		
		//-----------------------------------------
	    // Step 1: Read & Preprocess Price Data
		//-----------------------------------------
        //1. First, the pricing data and asset metadata needs to be loaded in as Resilient Distributed Datasets 
        //(RDDs), this is provided for you in the project template.
		// prices.javaRDD() converts Dataset<StockPrice> to JavaRDD<StockPrice> because RDD is a Spark low-level API.
		//.mapToPair(stock -> new Tuple2<>(stock.getStockTicker(), stock))
		//This mapToPair allows each stock (StockPrice) to be grouped by **Ticker.
		//.groupByKey() group StockPrice of the same Ticker into a group.
	    JavaPairRDD<String, StockPrice> priceData = prices
	            .javaRDD()
	            .mapToPair(stock -> new Tuple2<>(stock.getStockTicker(), stock));

	    JavaPairRDD<String, Iterable<StockPrice>> groupedPrices = priceData.groupByKey();
		
		//-----------------------------------------
	    //Step 2: Calculate the technical indicators of stocks
		//-----------------------------------------
		//-----------------------------------------
		// Data Collection at the Driver
		//-----------------------------------------
		
		JavaPairRDD<String, AssetFeatures> assetFeatures = groupedPrices.mapValues(stockPrices -> {
			// Collect our data as a list (Spark ACTION)
			//Because Iterable cannot be sorted directly, it is converted to ArrayList first.
			List<StockPrice> sortedStockPrices = new ArrayList<>();
			stockPrices.forEach(sortedStockPrices::add);

			// Sort by date to ensure that the data is ordered when calculating timing indicators.
			sortedStockPrices.sort(Comparator.comparing(StockPrice::getYear).thenComparing(StockPrice::getMonth)
					.thenComparing(StockPrice::getDay));
            //Get all Close Prices for calculating indicators
			List<Double> closingPrices = sortedStockPrices.stream().map(StockPrice::getClosePrice)
					.collect(Collectors.toList());
            //Get the latest 251 days' closing price
			int windowSize = Math.min(251, closingPrices.size());
			// Call Volitility directly to calculate volitility
			double volitility = (windowSize > 1)
					? Volitility.calculate(closingPrices.subList(closingPrices.size() - windowSize, closingPrices.size()))
					: Double.MAX_VALUE;//If the transaction data is too little (windowSize < 1), it is set to Double.MAX_VALUE, indicating high risk

			//Directly call the Returns calculation ROI provided by the teacher
			//Calculate the ROI over the past 5 days
			//If the data is less than 5 days, set it to -Double.MAX_VALUE, indicating that the risk is extremely high
			double roi = (closingPrices.size() >= 5) ? Returns.calculate(5, closingPrices) : -Double.MAX_VALUE;

			// Create AssetFeatures
			//Save ROI and Volatility in AssetFeatures for subsequent analysis
			AssetFeatures features = new AssetFeatures();
			features.setAssetReturn(roi);
			features.setAssetVolitility(volitility);
			return features;
		});
		//-----------------------------------------------------
		// Step 3: Filter assets that do not meet the criteria
		//-----------------------------------------------------
		//3. Third, after you have calculated these technical indicators, you will need to use them to filter 
		//the assets. You should filter out any assets that have a Volatility score greater than or equal to 4.
		
		//Connect to stock metadata (P/E Ratio, industry, etc.)
		//If the volatility is higher than volatileCeiling (too unstable), remove
		JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> filteredByVolatility = assetFeatures
				.join(assetMetadata)
				.filter(asset -> asset._2._1.getAssetVolitility() < volatilityCeiling);
		//------------------------------------------------------------------------------
		//Step 4: Filter out assets that are too high in P/E Ratio (not suitable for investment)
		//------------------------------------------------------------------------------
		JavaPairRDD<String, Asset> filteredAssets = filteredByVolatility.filter(asset -> {
			Tuple2<AssetFeatures, AssetMetadata> tuple = asset._2;
			double peRatio = tuple._2.getPriceEarningRatio();
			return peRatio > 0 && peRatio < peRatioThreshold && !Double.isNaN(peRatio);
		}).mapValues(asset -> {
			Tuple2<AssetFeatures, AssetMetadata> tuple = asset;
			AssetFeatures features = tuple._1;
			AssetMetadata metadata = tuple._2;
			features.setPeRatio(metadata.getPriceEarningRatio());

			return new Asset(metadata.getSymbol(), features, metadata.getName(), metadata.getIndustry(),
					metadata.getSector());
		});
        //-----------------------------------------
		// Step 5: Sort by yield, return to Top 5
		//-----------------------------------------
		List<Asset> top5Assets = filteredAssets.values().sortBy(asset -> asset.getFeatures().getAssetReturn(), false, 1)
				.take(5);

		return new AssetRanking(top5Assets.toArray(new Asset[5]));
	}

}
