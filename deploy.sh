gcloud config set project mydeal-bigquery
gcloud services enable cloudbuild.googleapis.com
gcloud functions deploy fb_ads_insights \
     --entry-point social_media_fb \
    --runtime python39 \
    --trigger-topic trigger_fb_ads_insights\
    --verbosity debug \
    --timeout 540s \
    --memory 512MB   \
