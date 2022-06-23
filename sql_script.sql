-- Construct an account to anonymous ID mapping
with account_mappings as (
select distinct on (anonymous_id)
  anonymous_id,
  account_id
from app_production.identifies
where anonymous_id is not null
order by anonymous_id, account_id
),

-- Get the time that each account first subscribed
sign_ups as (
select id as account_id, name, email, created_at from production.accounts
)

-- Get the first and last click id from the set of clicks before sign up
select distinct on (account_id)
  ad_clicks.anonymous_id,
  first_value(ad_clicks.id) over browser_clicks as first_click_id,
  last_value(ad_clicks.id) over browser_clicks as last_click_id,
  sign_ups.account_id,
  coalesce(sign_ups.name, sign_ups.email) as name,
  sign_ups.created_at as signed_up_at
from derived.ad_clicks
inner join account_mappings on account_mappings.anonymous_id = ad_clicks.anonymous_id
inner join sign_ups on sign_ups.account_id = account_mappings.account_id
where
  ad_clicks.received_at between
  (sign_ups.created_at - interval '21 days') and sign_ups.created_at
window browser_clicks as (partition by account_mappings.account_id order by ad_clicks.received_at)
order by account_id, ad_clicks.received_at desc
clicks.sql
with pages_with_ad_ids as (
select
id,
anonymous_id,
received_at,
substring(search, '.*[?&]gclid=([^$&]*)') as gcl_id,
substring(search, '.*[?&]fbad_id=([^$&]*)') as fbad_id
from marketing.pages
)

select
p.id,
p.received_at,
p.anonymous_id,
case
  when p.gcl_id is not null and p.fbad_id is null then 'Google'
  when p.fbad_id is not null and p.gcl_id is null then 'Facebook'
  else 'wtf'
end as platform,
case
  when p.gcl_id is not null then coalesce(g3.name, '[unknown]')
  when p.fbad_id is not null then coalesce(f3.name, '[unknown]')
  else 'wtf'
end as campaign_name,
case
  when p.gcl_id is not null then coalesce(g2.name, '[unknown]')
  when p.fbad_id is not null then coalesce(f2.name, '[unknown]')
  else 'wtf'
end as ad_group_name
from pages_with_ad_ids p
left join adwords.click_performance_reports g1 on g1.gcl_id = p.gcl_id
left join adwords.ad_groups g2 on g2.id = g1.ad_group_id
left join adwords.campaigns g3 on g3.id = g2.campaign_id
left join facebookads.ads f1 on f1.id = p.fbad_id
left join facebookads.ad_sets f2 on f2.id = f1.adset_id
left join facebookads.campaigns f3 on f3.id = f2.campaign_id
where p.gcl_id is not null or p.fbad_id is not null


---Campaign Manager 360 Data Transfer queries
---Match Floodlight variables with temp tables
---Generate a match between user_id and custom Floodlight variables in the activity table. This can then be used to join first-party data with Campaign Manager 360 data.



/* Creating the match temp table. This can be a separate query and the
temporary table will persist for 72 hours. */

CREATE TABLE
  temp_table AS (
  SELECT
    user_id,
    REGEXP_EXTRACT(event.other_data, 'u1=([^;]*)') AS u1_val
  FROM
    adh.cm_dt_activities_attributed
  GROUP BY
    1,
    2 )

/* Matching to Campaign Manager 360 impression data */

SELECT
  imp.event.campaign_id,
  temp.u1_val,
  COUNT(*) AS cnt
FROM
  adh.cm_dt_impressions AS imp
JOIN
  tmp.temp_table AS temp USING (user_id)
GROUP BY
  1,
  2

---Impression delivery
---This example is good for impression management, and shows how to find the number of impressions that were served beyond frequency caps or if certain prospects were underexposed to ads. Use this knowledge to optimize your sites and tactics to get the right number of impressions in front of a chosen audience.


/* For this query to run, @advertiser_ids and @campaigns_ids
must be replaced with actual IDs. For example [12345] */

WITH filtered_uniques AS (
  SELECT
    user_id,
    COUNT(event.placement_id) AS frequency
  FROM adh.cm_dt_impressions
  WHERE user_id != '0'
    AND event.advertiser_id IN UNNEST(@advertiser_ids)
    AND event.campaign_id IN UNNEST(@campaign_ids)
    AND event.country_domain_name = 'US'
  GROUP BY user_id
)
SELECT
  frequency,
  COUNT(*) AS uniques
FROM filtered_uniques
GROUP BY frequency
ORDER BY frequency
;

---Total unique cookie count / frequency
---This example helps identify tactics and ad formats that lead to increases or decreases in unique cookie count or frequency.


/* For this query to run, @advertiser_ids and @campaigns_ids and @placement_ids
must be replaced with actual IDs. For example [12345] */

SELECT
  COUNT(DISTINCT user_id) AS total_users,
  COUNT(DISTINCT event.site_id) AS total_sites,
  COUNT(DISTINCT device_id_md5) AS total_devices,
  COUNT(event.placement_id) AS impressions
FROM adh.cm_dt_impressions
WHERE user_id != '0'
  AND event.advertiser_id IN UNNEST(@advertiser_ids)
  AND event.campaign_id IN UNNEST(@campaign_ids)
  AND event.placement_id IN UNNEST(@placement_ids)
  AND event.country_domain_name = 'US'
;

---You can also include site or placement IDs in the WHERE clause to narrow your query.

---Total unique cookie count and average frequency by state
---This example joins the cm_dt_impressions table and the cm_dt_state metadata table to show total impressions, cookie counts per state, and average impression by user, grouped by North America geographic state or province.



WITH impression_stats AS (
  SELECT
    event.country_domain_name AS country,
    CONCAT(event.country_domain_name, '-', event.state) AS state,
    COUNT(DISTINCT user_id) AS users,
    COUNT(*) AS impressions
  FROM adh.cm_dt_impressions
  WHERE event.country_domain_name = 'US'
    OR event.country_domain_name = 'CA'
  GROUP BY 1, 2
)
SELECT
  country,
  IFNULL(state_name, state) AS state_name,
  users,
  impressions,
  FORMAT(
    '%0.2f',
    IF(
      IFNULL(impressions, 0) = 0,
      0,
      impressions / users
    )
  ) AS avg_imps_per_user
FROM impression_stats
LEFT JOIN adh.cm_dt_state USING (state)
;

---Mobile app impressions with _rdid tables
---The following queries must be done separately. RDID and non-RDID tables cannot be joined in the same query (even using temp tables).

---Query 1:



SELECT
  event.campaign_id,
  event.placement_id,
  event.country_domain_name,
  COUNT(*) AS impressions,
  COUNT(DISTINCT user_id) AS users
FROM adh.cm_dt_impressions
WHERE is_app_traffic
GROUP BY 1, 2, 3
;

Query 2:



SELECT
  event.campaign_id,
  event.placement_id,
  event.country_domain_name,
  COUNT(DISTINCT device_id_md5) AS device_ids
FROM adh.cm_dt_impressions_rdid
GROUP BY 1, 2, 3
;

---The results can be joined using campaign_id, placement_id, and country_domain_name.

---Display and Video 360 audiences
---This example shows how to analyze Display and Video 360 audiences. Learn which audiences impressions are reaching, and determine if some audiences perform better than others. This knowledge can help balance unique cookie count (putting ads in front of a lot of users) and quality (narrow targeting and viewable impressions), depending on your goals.


/* For this query to run, @advertiser_ids and @campaigns_ids and @placement_ids
must be replaced with actual IDs. For example [12345] */

WITH filtered_impressions AS (
  SELECT
    event.event_time as date,
    CASE
      WHEN (event.browser_enum IN ('29', '30', '31')
            OR event.os_id IN
              (501012, 501013, 501017, 501018,
               501019, 501020, 501021, 501022,
               501023, 501024, 501025, 501027))
      THEN 'Mobile'
      ELSE 'Desktop'
    END AS device,
    event.dv360_matching_targeted_segments,
    event.active_view_viewable_impressions,
    event.active_view_measurable_impressions,
    user_id
  FROM adh.cm_dt_impressions
  WHERE event.dv360_matching_targeted_segments != ''
    AND event.advertiser_id in UNNEST(@advertiser_ids)
    AND event.campaign_id IN UNNEST(@campaign_ids)
    AND event.dv360_country_code = 'US'
)
SELECT
  audience_id,
  device,
  COUNT(*) AS impressions,
  COUNT(DISTINCT user_id) AS uniques,
  ROUND(COUNT(*) / COUNT(DISTINCT user_id), 1) AS frequency,
  SUM(active_view_viewable_impressions) AS viewable_impressions,
  SUM(active_view_measurable_impressions) AS measurable_impressions
FROM filtered_impressions
JOIN UNNEST(SPLIT(dv360_matching_targeted_segments, ' ')) AS audience_id
GROUP BY 1, 2
;

---Viewability
---These example show how to measure Active View Plus viewability metrics.



WITH T AS (
   SELECT cm_dt_impressions.event.impression_id AS Impression,
          cm_dt_impressions.event.active_view_measurable_impressions AS AV_Measurable,
          SUM(cm_dt_active_view_plus.event.active_view_plus_measurable_count) AS AVP_Measurable
     FROM adh.cm_dt_impressions
FULL JOIN adh.cm_dt_active_view_plus
          ON (cm_dt_impressions.event.impression_id =
              cm_dt_active_view_plus.event.impression_id)
    GROUP BY Impression, AV_Measurable
)
SELECT COUNT(Impression), SUM(AV_Measurable), SUM(AVP_Measurable)
  FROM T
;



WITH Raw AS (
  SELECT
    event.ad_id AS Ad_Id,
  SUM(event.active_view_plus_measurable_count) AS avp_total,
  SUM(event.active_view_first_quartile_viewable_impressions) AS avp_1st_quartile,
  SUM(event.active_view_midpoint_viewable_impressions) AS avp_2nd_quartile,
  SUM(event.active_view_third_quartile_viewable_impressions) AS avp_3rd_quartile,
  SUM(event.active_view_complete_viewable_impressions) AS avp_complete
  FROM
    adh.cm_dt_active_view_plus
  GROUP BY
    1
)

SELECT
  Ad_Id,
  avp_1st_quartile / avp_total AS Viewable_Rate_1st_Quartile,
  avp_2nd_quartile / avp_total AS Viewable_Rate_2nd_Quartile,
    avp_3rd_quartile / avp_total AS Viewable_Rate_3rd_Quartile,
    avp_complete / avp_total AS Viewable_Rate_Completion_Quartile
FROM
  Raw
WHERE
  avp_total > 0
ORDER BY
  Viewable_Rate_1st_Quartile DESC
;

---Dynamic data in Campaign Manager 360 Data Transfer
---Number of impressions per dynamic profile and feed

SELECT
  event.dynamic_profile,
  feed_name,
  COUNT(*) as impressions
FROM adh.cm_dt_impressions
JOIN UNNEST (event.feed) as feed_name
GROUP BY 1, 2;
Number of impressions per dynamic reporting label in feed 1

SELECT
  event.feed_reporting_label[SAFE_ORDINAL(1)] feed1_reporting_label,,
  COUNT(*) as impressions
FROM adh.cm_dt_impressions
WHERE event.feed_reporting_label[SAFE_ORDINAL(1)] <> “” # where you have at least one reporting label set
GROUP BY 1;
Number of impressions where the reporting label = ‘red’ in feed 2

SELECT
  event.feed_reporting_label[SAFE_ORDINAL(2)] AS feed1_reporting_label,
  COUNT(*) as impressions
FROM adh.cm_dt_impressions
WHERE event.feed_reporting_label[SAFE_ORDINAL(2)] = “red”
GROUP BY 1;
Number of impressions where reporting dimension_1 = ‘red’ and reporting dimension_2 = ‘car’ in feed 1

SELECT
  event.feed_reporting_label[SAFE_ORDINAL(1)] AS feed1_reporting_label,
  event.feed_reporting_dimension1[SAFE_ORDINAL(1)] AS feed1_reporting_dimension1,
  event.feed_reporting_dimension2[SAFE_ORDINAL(1)] AS feed2_reporting_dimension1,
  event.feed_reporting_dimension3[SAFE_ORDINAL(1)] AS feed3_reporting_dimension1,
  event.feed_reporting_dimension4[SAFE_ORDINAL(1)] AS feed4_reporting_dimension1,
  event.feed_reporting_dimension5[SAFE_ORDINAL(1)] AS feed5_reporting_dimension1,
  event.feed_reporting_dimension6[SAFE_ORDINAL(1)] AS feed6_reporting_dimension1,
  COUNT(*) as impressions
FROM adh.cm_dt_impressions
WHERE event.feed_reporting_dimension1[SAFE_ORDINAL(1)] = “red”
AND event.feed_reporting_dimension2[SAFE_ORDINAL(1)] = “car”
GROUP BY 1,2,3,4,5,6,7;
---Ad Formats in Campaign Manager 360 Data Transfer
---These examples show how to determine which ad formats are maximizing unique cookie count or frequency of impressions. Use this knowledge to help balance total unique cookie count and user exposure to ads.

--Impression delivery

/* For this query to run, @advertiser_ids and @campaigns_ids
must be replaced with actual IDs. For example [12345]. YOUR_BQ_DATASET must be
replaced with the actual name of your dataset.*/

WITH filtered_uniques AS (
  SELECT
    user_id,
    CASE
      WHEN creative_type LIKE '%Video%' THEN 'Video'
      WHEN creative_type IS NULL THEN 'Unknown'
      ELSE 'Display'
    END AS creative_format,
    COUNT(*) AS impressions
  FROM adh.cm_dt_impressions impression
  LEFT JOIN YOUR_BQ_DATASET.campaigns creative
    ON creative.rendering_id = impression.event.rendering_id
  WHERE user_id != '0'
    AND event.advertiser_id IN UNNEST(@advertiser_ids)
    AND event.campaign_id IN UNNEST(@campaign_ids)
    AND event.country_domain_name = 'US'
  GROUP BY user_id, creative_format
)
SELECT
  impressions AS frequency,
  creative_format,
  COUNT(DISTINCT user_id) AS uniques,
  SUM(impressions) AS impressions
FROM filtered_uniques
GROUP BY frequency, creative_format
ORDER BY frequency
;

---Unique cookie count and frequency

/* For this query to run, @advertiser_ids and @campaigns_ids
must be replaced with actual IDs. For example [12345]. YOUR_BQ_DATASET must be
replaced with the actual name of your dataset. */

WITH filtered_impressions AS (
  SELECT
    event.campaign_id AS campaign_id,
    event.rendering_id AS rendering_id,
    user_id
  FROM adh.cm_dt_impressions
  WHERE user_id != '0'
    AND event.advertiser_id IN UNNEST(@advertiser_ids)
    AND event.campaign_id IN UNNEST(@campaign_ids)
    AND event.country_domain_name = 'US'
)
SELECT
  Campaign,
  CASE
    WHEN creative_type LIKE '%Video%' THEN 'Video'
    WHEN creative_type IS NULL THEN 'Unknown'
    ELSE 'Display'
  END AS creative_format,
  COUNT(DISTINCT user_id) AS users,
  COUNT(*) AS impressions
FROM filtered_impressions
LEFT JOIN YOUR_BQ_DATASET.campaigns USING (campaign_id)
LEFT JOIN YOUR_BQ_DATASET.creatives USING (rendering_id)
GROUP BY 1, 2
;

---Google Ads
---Mobile app impressions with _rdid tables
--Query 1:



SELECT
  campaign_id,
  COUNT(*) AS imp,
  COUNT(DISTINCT user_id) AS users
FROM adh.google_ads_impressions
WHERE is_app_traffic
GROUP BY 1
;

--Query 2:


SELECT
  campaign_id,
  COUNT(DISTINCT device_id_md5) AS device_ids
FROM adh.google_ads_impressions_rdid
GROUP BY 1
;

---The results can be joined using campaign_id.

---Demographic delivery
---This example shows how to determine which campaigns are reaching a given demographic.

/* For this query to run, @customer_id
must be replaced with an actual ID. For example [12345] */

WITH impression_stats AS (
  SELECT
    campaign_id,
    demographics.gender AS gender_id,
    demographics.age_group AS age_group_id,
    COUNT(DISTINCT user_id) AS users,
    COUNT(*) AS impressions
  FROM adh.google_ads_impressions
  WHERE customer_id = @customer_id
  GROUP BY 1, 2, 3
)
SELECT
  campaign_name,
  gender_name,
  age_group_name,
  users,
  impressions
FROM impression_stats
LEFT JOIN adh.google_ads_campaign USING (campaign_id)
LEFT JOIN adh.gender USING (gender_id)
LEFT JOIN adh.age_group USING (age_group_id)
ORDER BY 1, 2, 3
;

---Subtracting one group of users from another
---This example shows how to subtract one group of users from another. This technique has a wide range of applications, including counting non-converters, users with no viewable impressions, and users with no clicks.


WITH exclude AS (
  SELECT DISTINCT user_id
  FROM adh.google_ads_impressions
  WHERE campaign_id = 123
)

SELECT
  COUNT(DISTINCT imp.user_id) -
      COUNT(DISTINCT exclude.user_id) AS users
FROM adh.google_ads_impressions imp
LEFT JOIN exclude
  USING (user_id)
WHERE imp.campaign_id = 876
;

---Viewability
---This example shows how to run a simple viewability query. Viewability provides a signal of how likely it is that a user actually saw your ad.

WITH
active_view_metrics AS (
  SELECT
    SUM(imp.num_active_view_eligible_impression) AS EligibleImpressions,
    SUM(imp.num_active_view_measurable_impression) AS MeasurableImpressions,
    SUM(view.num_active_view_viewable_impression) AS ViewableImpressions,
    SUM(IF(imp.num_active_view_eligible_impression > 0 AND (ctv.template_id IN (213) OR ctv.creative_type in (84)), 1, 0)) AS EligibleSkippableImpressions
  FROM adh.google_ads_impressions imp
  LEFT JOIN adh.google_ads_active_views view ON view.impression_id = imp.impression_id
  LEFT JOIN adh.google_ads_adgroupcreative agc ON imp.ad_group_creative_id = agc.ad_group_creative_id
  LEFT JOIN adh.google_ads_creative AS ctv ON agc.creative_id = ctv.creative_id
  WHERE (imp.active_view_type = 'VIDEO_MEASURABLE' OR imp.active_view_type = 'VIDEO_ENABLED')
  AND imp.customer_id = @customer_id AND imp.campaign_id = @campaign_id
)
SELECT
  EligibleImpressions,
  MeasurableImpressions,
  ViewableImpressions,
  EligibleSkippableImpressions
FROM active_view_metrics
;
---Google Ads advertiser time zone settings
SELECT
  customer_id,
  customer_timezone,
  count(1) as impressions
FROM adh.google_ads_impressions i
  INNER JOIN adh.google_ads_customer c
    ON c.customer_id = i.customer_id
WHERE TIMESTAMP_MICROS(i.query_id.time_usec) >= CAST(DATETIME(@date, c.customer_timezone) AS TIMESTAMP)
AND TIMESTAMP_MICROS(i.query_id.time_usec) < CAST(DATETIME_ADD(DATETIME(@date, c.customer_timezone), INTERVAL 1 DAY) AS TIMESTAMP)
GROUP BY customer_id, customer_timezone
---YouTube ad pod queries
---Ad pods group 2 ads into a single ad-break during longer YouTube viewing sessions. (Think commercial break, but limited to 2 ads.) Ads served in ad pods remain skippable. However, if a user skips the first ad, the second ad is also skipped.

---GoogleAds Trueview Instream campaign impression and trueview views
SELECT
 cmp.campaign_name,
 imp.is_app_traffic,
 COUNT(*) AS total_impressions,
 COUNTIF(clk.click_id IS NOT NULL) AS total_trueview_views
FROM adh.google_ads_impressions imp
JOIN adh.google_ads_campaign cmp USING (campaign_id)
JOIN adh.google_ads_adgroup adg USING (adgroup_id)
LEFT JOIN adh.google_ads_clicks clk ON
  imp.impression_id = clk.impression_id
WHERE
 imp.customer_id IN UNNEST(@customer_ids)
 AND adg.adgroup_type = 'VIDEO_TRUE_VIEW_IN_STREAM'
 AND cmp.advertising_channel_type = 'VIDEO'
GROUP BY 1, 2
Display and Video 360 viewability metrics by line items
WITH
 imp_stats AS (
   SELECT
     imp.line_item_id,
     count(*) as total_imp,
     SUM(num_active_view_measurable_impression) AS num_measurable_impressions,
     SUM(num_active_view_eligible_impression) AS num_enabled_impressions
   FROM adh.dv360_youtube_impressions imp
   WHERE
     imp.line_item_id IN UNNEST(@line_item_ids)
   GROUP BY 1
 ),
 av_stats AS (
   SELECT
     imp.line_item_id,
     SUM(num_active_view_viewable_impression) AS num_viewable_impressions
   FROM adh.dv360_youtube_impressions imp
   LEFT JOIN
     adh.dv360_youtube_active_views av
     ON imp.impression_id = av.impression_id
   WHERE
     imp.line_item_id IN UNNEST(@line_item_ids)
   GROUP BY 1
 )
SELECT
 li.line_item_name,
 SUM(imp.total_imp) as num_impressions,
 SUM(imp.num_measurable_impressions) AS num_measurable_impressions,
 SUM(imp.num_enabled_impressions) AS num_enabled_impressions,
 SUM(IFNULL(av.num_viewable_impressions, 0)) AS num_viewable_impressions
FROM imp_stats as imp
LEFT JOIN av_stats AS av USING (line_item_id)
JOIN adh.dv360_youtube_lineitem li ON (imp.line_item_id = li.line_item_id)
GROUP BY 1
---YouTube Reserve queries
--Impression delivery by advertiser
---This query measures the number of impressions and distinct users per advertiser. You can use these numbers to calculate the average number of impressions per user (or "ad frequency").

SELECT
  advertiser_name,
  COUNT(*) AS imp,
  COUNT(DISTINCT user_id) AS users
FROM adh.yt_reserve_impressions AS impressions
JOIN adh.yt_reserve_order order ON impressions.order_id = order.order_id
GROUP BY 1
;
---Ad skips
---This query measures the number of ad skips per customer, campaign, ad group, and creative.

SELECT
  impression_data.customer_id,
  impression_data.campaign_id,
  impression_data.adgroup_id,
  impression_data.ad_group_creative_id,
  COUNTIF(label = "videoskipped") AS num_skips
FROM
  adh.google_ads_conversions
GROUP BY 1, 2, 3, 4;


/* For this query to run, @campaign_1 and @campaign_2 must be replaced with
actual campaign IDs. */

WITH flagged_impressions AS (
SELECT
  user_ID,
  SUM(IF(campaign_ID in UNNEST(@campaign_1), 1, 0)) AS C1_impressions,
  SUM(IF(campaign_ID in UNNEST(@campaign_2), 1, 0)) AS C2_impressions
FROM adh.cm_dt_impressions
GROUP BY user_ID

SELECT COUNTIF(C1_impressions > 0) as C1_cookie_count,
 COUNTIF(C2_impressions > 0) as C2_cookie_count,
 COUNTIF(C1_impressions > 0 and C2_impressions > 0) as overlap_cookie_count
FROM flagged_impressions
;
Partner Sold - Cross Sell
This query measures impressions and click-throughs of partner-sold inventory.

SELECT
  a.record_date AS record_date,
  a.line_item_id AS line_item_id,
  a.creative_id AS creative_id,
  a.ad_id AS ad_id,
  a.impressions AS impressions,
  a.click_through AS click_through,
  a.video_skipped AS video_skipped,
  b.pixel_url AS pixel_url
FROM
  (
    SELECT
      FORMAT_TIMESTAMP('%D', TIMESTAMP_MICROS(i.query_id.time_usec), 'Etc/UTC') AS record_date,
      i.line_item_id as line_item_id,
      i.creative_id as creative_id,
      i.ad_id as ad_id,
      COUNT(i.query_id) as impressions,
      COUNTIF(c.label='video_click_to_advertiser_site') AS click_through,
      COUNTIF(c.label='videoskipped') AS video_skipped
    FROM
      adh.partner_sold_cross_sell_impressions AS i
      LEFT JOIN adh.partner_sold_cross_sell_conversions AS c
        ON i.impression_id = c.impression_id
    GROUP BY
      1, 2, 3, 4
    ) AS a
    JOIN adh.partner_sold_cross_sell_creative_pixels AS b
      ON (a.ad_id = b.ad_id)
;
App store impressions
The following query counts the total number of impressions grouped by app store and app.

SELECT app_store_name, app_name, COUNT(*) AS number
FROM adh.google_ads_impressions AS imp
JOIN adh.mobile_app_info
USING (app_store_id, app_id)
WHERE imp.app_id IS NOT NULL
GROUP BY 1,2
ORDER BY 3 DESC