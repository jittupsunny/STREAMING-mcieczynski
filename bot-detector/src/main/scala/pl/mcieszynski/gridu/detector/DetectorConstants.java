package pl.mcieszynski.gridu.detector;

public interface DetectorConstants {

    int timeRatio = 10;

    int BOT_RATIO_LIMIT = 5;

    int REQUEST_LIMIT = 1000 / timeRatio;

    int CATEGORY_TYPE_LIMIT = 5;

    /**
     * all times in seconds
     */
    long TIME_WINDOW_LIMIT = 600 / timeRatio;

    long BATCH_DURATION = 60 / timeRatio;

    long SLIDE_DURATION = 600 / timeRatio;

    String REQUESTS = "requests";

    String CATEGORIES = "categories";

    String RATIO = "ratio";

}
