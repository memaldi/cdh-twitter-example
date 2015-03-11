package eu.deustotech.internet.flume.sink.test;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Iterator;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 11/03/15.
 */
public class TestTwitterHBaseSink {

    public static void main(String args[]) {
        String jsonString = "{\n" +
                "    \"filter_level\": \"low\",\n" +
                "    \"retweeted\": false,\n" +
                "    \"in_reply_to_screen_name\": null,\n" +
                "    \"possibly_sensitive\": false,\n" +
                "    \"truncated\": false,\n" +
                "    \"lang\": \"es\",\n" +
                "    \"in_reply_to_status_id_str\": null,\n" +
                "    \"id\": 574840856303300600,\n" +
                "    \"extended_entities\": {\n" +
                "        \"media\": [\n" +
                "            {\n" +
                "                \"sizes\": {\n" +
                "                    \"thumb\": {\n" +
                "                        \"w\": 150,\n" +
                "                        \"resize\": \"crop\",\n" +
                "                        \"h\": 150\n" +
                "                    },\n" +
                "                    \"small\": {\n" +
                "                        \"w\": 340,\n" +
                "                        \"resize\": \"fit\",\n" +
                "                        \"h\": 255\n" +
                "                    },\n" +
                "                    \"large\": {\n" +
                "                        \"w\": 1024,\n" +
                "                        \"resize\": \"fit\",\n" +
                "                        \"h\": 768\n" +
                "                    },\n" +
                "                    \"medium\": {\n" +
                "                        \"w\": 600,\n" +
                "                        \"resize\": \"fit\",\n" +
                "                        \"h\": 450\n" +
                "                    }\n" +
                "                },\n" +
                "                \"id\": 574666182252458000,\n" +
                "                \"media_url_https\": \"https://pbs.twimg.com/media/B_mf2RmWcAApvbF.jpg\",\n" +
                "                \"media_url\": \"http://pbs.twimg.com/media/B_mf2RmWcAApvbF.jpg\",\n" +
                "                \"expanded_url\": \"http://twitter.com/Juanternero/status/574666182323793920/photo/1\",\n" +
                "                \"source_status_id_str\": \"574666182323793920\",\n" +
                "                \"indices\": [\n" +
                "                    124,\n" +
                "                    140\n" +
                "                ],\n" +
                "                \"source_status_id\": 574666182323793900,\n" +
                "                \"id_str\": \"574666182252457984\",\n" +
                "                \"type\": \"photo\",\n" +
                "                \"display_url\": \"pic.twitter.com/zFVWPsiqrW\",\n" +
                "                \"url\": \"http://t.co/zFVWPsiqrW\"\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"in_reply_to_user_id_str\": null,\n" +
                "    \"timestamp_ms\": \"1425887714826\",\n" +
                "    \"in_reply_to_status_id\": null,\n" +
                "    \"created_at\": \"Mon Mar 09 07:55:14 +0000 2015\",\n" +
                "    \"favorite_count\": 0,\n" +
                "    \"place\": null,\n" +
                "    \"coordinates\": null,\n" +
                "    \"text\": \"RT @Juanternero: Ver a gente ilusionada con la política me alegra mucho. Colgando carteles de @CiudadanosCs #YoSoyNaranjito http://t.co/zFV…\",\n" +
                "    \"contributors\": null,\n" +
                "    \"retweeted_status\": {\n" +
                "        \"filter_level\": \"low\",\n" +
                "        \"retweeted\": false,\n" +
                "        \"in_reply_to_screen_name\": null,\n" +
                "        \"possibly_sensitive\": false,\n" +
                "        \"truncated\": false,\n" +
                "        \"lang\": \"es\",\n" +
                "        \"in_reply_to_status_id_str\": null,\n" +
                "        \"id\": 574666182323793900,\n" +
                "        \"extended_entities\": {\n" +
                "            \"media\": [\n" +
                "                {\n" +
                "                    \"sizes\": {\n" +
                "                        \"thumb\": {\n" +
                "                            \"w\": 150,\n" +
                "                            \"resize\": \"crop\",\n" +
                "                            \"h\": 150\n" +
                "                        },\n" +
                "                        \"small\": {\n" +
                "                            \"w\": 340,\n" +
                "                            \"resize\": \"fit\",\n" +
                "                            \"h\": 255\n" +
                "                        },\n" +
                "                        \"large\": {\n" +
                "                            \"w\": 1024,\n" +
                "                            \"resize\": \"fit\",\n" +
                "                            \"h\": 768\n" +
                "                        },\n" +
                "                        \"medium\": {\n" +
                "                            \"w\": 600,\n" +
                "                            \"resize\": \"fit\",\n" +
                "                            \"h\": 450\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"id\": 574666182252458000,\n" +
                "                    \"media_url_https\": \"https://pbs.twimg.com/media/B_mf2RmWcAApvbF.jpg\",\n" +
                "                    \"media_url\": \"http://pbs.twimg.com/media/B_mf2RmWcAApvbF.jpg\",\n" +
                "                    \"expanded_url\": \"http://twitter.com/Juanternero/status/574666182323793920/photo/1\",\n" +
                "                    \"indices\": [\n" +
                "                        107,\n" +
                "                        129\n" +
                "                    ],\n" +
                "                    \"id_str\": \"574666182252457984\",\n" +
                "                    \"type\": \"photo\",\n" +
                "                    \"display_url\": \"pic.twitter.com/zFVWPsiqrW\",\n" +
                "                    \"url\": \"http://t.co/zFVWPsiqrW\"\n" +
                "                }\n" +
                "            ]\n" +
                "        },\n" +
                "        \"in_reply_to_user_id_str\": null,\n" +
                "        \"in_reply_to_status_id\": null,\n" +
                "        \"created_at\": \"Sun Mar 08 20:21:09 +0000 2015\",\n" +
                "        \"favorite_count\": 3,\n" +
                "        \"place\": null,\n" +
                "        \"coordinates\": null,\n" +
                "        \"text\": \"Ver a gente ilusionada con la política me alegra mucho. Colgando carteles de @CiudadanosCs #YoSoyNaranjito http://t.co/zFVWPsiqrW\",\n" +
                "        \"contributors\": null,\n" +
                "        \"geo\": null,\n" +
                "        \"entities\": {\n" +
                "            \"trends\": [],\n" +
                "            \"symbols\": [],\n" +
                "            \"urls\": [],\n" +
                "            \"hashtags\": [\n" +
                "                {\n" +
                "                    \"text\": \"YoSoyNaranjito\",\n" +
                "                    \"indices\": [\n" +
                "                        91,\n" +
                "                        106\n" +
                "                    ]\n" +
                "                }\n" +
                "            ],\n" +
                "            \"media\": [\n" +
                "                {\n" +
                "                    \"sizes\": {\n" +
                "                        \"thumb\": {\n" +
                "                            \"w\": 150,\n" +
                "                            \"resize\": \"crop\",\n" +
                "                            \"h\": 150\n" +
                "                        },\n" +
                "                        \"small\": {\n" +
                "                            \"w\": 340,\n" +
                "                            \"resize\": \"fit\",\n" +
                "                            \"h\": 255\n" +
                "                        },\n" +
                "                        \"large\": {\n" +
                "                            \"w\": 1024,\n" +
                "                            \"resize\": \"fit\",\n" +
                "                            \"h\": 768\n" +
                "                        },\n" +
                "                        \"medium\": {\n" +
                "                            \"w\": 600,\n" +
                "                            \"resize\": \"fit\",\n" +
                "                            \"h\": 450\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"id\": 574666182252458000,\n" +
                "                    \"media_url_https\": \"https://pbs.twimg.com/media/B_mf2RmWcAApvbF.jpg\",\n" +
                "                    \"media_url\": \"http://pbs.twimg.com/media/B_mf2RmWcAApvbF.jpg\",\n" +
                "                    \"expanded_url\": \"http://twitter.com/Juanternero/status/574666182323793920/photo/1\",\n" +
                "                    \"indices\": [\n" +
                "                        107,\n" +
                "                        129\n" +
                "                    ],\n" +
                "                    \"id_str\": \"574666182252457984\",\n" +
                "                    \"type\": \"photo\",\n" +
                "                    \"display_url\": \"pic.twitter.com/zFVWPsiqrW\",\n" +
                "                    \"url\": \"http://t.co/zFVWPsiqrW\"\n" +
                "                }\n" +
                "            ],\n" +
                "            \"user_mentions\": [\n" +
                "                {\n" +
                "                    \"id\": 19028805,\n" +
                "                    \"name\": \"Ciudadanos \",\n" +
                "                    \"indices\": [\n" +
                "                        77,\n" +
                "                        90\n" +
                "                    ],\n" +
                "                    \"screen_name\": \"CiudadanosCs\",\n" +
                "                    \"id_str\": \"19028805\"\n" +
                "                }\n" +
                "            ]\n" +
                "        },\n" +
                "        \"source\": \"<a href=\\\"http://twitter.com/download/iphone\\\" rel=\\\"nofollow\\\">Twitter for iPhone</a>\",\n" +
                "        \"favorited\": false,\n" +
                "        \"in_reply_to_user_id\": null,\n" +
                "        \"retweet_count\": 2,\n" +
                "        \"id_str\": \"574666182323793920\",\n" +
                "        \"user\": {\n" +
                "            \"location\": \"\",\n" +
                "            \"default_profile\": false,\n" +
                "            \"profile_background_tile\": true,\n" +
                "            \"statuses_count\": 700,\n" +
                "            \"lang\": \"es\",\n" +
                "            \"profile_link_color\": \"0084B4\",\n" +
                "            \"profile_banner_url\": \"https://pbs.twimg.com/profile_banners/291041963/1425456708\",\n" +
                "            \"id\": 291041963,\n" +
                "            \"following\": null,\n" +
                "            \"protected\": false,\n" +
                "            \"favourites_count\": 24,\n" +
                "            \"profile_text_color\": \"333333\",\n" +
                "            \"verified\": false,\n" +
                "            \"description\": \"Sevillista, aficionado al fútbol y al boxeo.\",\n" +
                "            \"contributors_enabled\": false,\n" +
                "            \"profile_sidebar_border_color\": \"FFFFFF\",\n" +
                "            \"name\": \"Juan Ternero\",\n" +
                "            \"profile_background_color\": \"C0DEED\",\n" +
                "            \"created_at\": \"Sun May 01 11:22:25 +0000 2011\",\n" +
                "            \"default_profile_image\": false,\n" +
                "            \"followers_count\": 95,\n" +
                "            \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/499824969573232640/wrd5D6y8_normal.jpeg\",\n" +
                "            \"geo_enabled\": false,\n" +
                "            \"profile_background_image_url\": \"http://pbs.twimg.com/profile_background_images/615848505/vy51fxo7nti4es5ofqt3.jpeg\",\n" +
                "            \"profile_background_image_url_https\": \"https://pbs.twimg.com/profile_background_images/615848505/vy51fxo7nti4es5ofqt3.jpeg\",\n" +
                "            \"follow_request_sent\": null,\n" +
                "            \"url\": \"http://www.fdfutbol.com\",\n" +
                "            \"utc_offset\": 7200,\n" +
                "            \"time_zone\": \"Athens\",\n" +
                "            \"notifications\": null,\n" +
                "            \"profile_use_background_image\": true,\n" +
                "            \"friends_count\": 158,\n" +
                "            \"profile_sidebar_fill_color\": \"DDEEF6\",\n" +
                "            \"screen_name\": \"Juanternero\",\n" +
                "            \"id_str\": \"291041963\",\n" +
                "            \"profile_image_url\": \"http://pbs.twimg.com/profile_images/499824969573232640/wrd5D6y8_normal.jpeg\",\n" +
                "            \"listed_count\": 0,\n" +
                "            \"is_translator\": false\n" +
                "        }\n" +
                "    },\n" +
                "    \"geo\": null,\n" +
                "    \"entities\": {\n" +
                "        \"trends\": [],\n" +
                "        \"symbols\": [],\n" +
                "        \"urls\": [],\n" +
                "        \"hashtags\": [\n" +
                "            {\n" +
                "                \"text\": \"YoSoyNaranjito\",\n" +
                "                \"indices\": [\n" +
                "                    108,\n" +
                "                    123\n" +
                "                ]\n" +
                "            }\n" +
                "        ],\n" +
                "        \"media\": [\n" +
                "            {\n" +
                "                \"sizes\": {\n" +
                "                    \"thumb\": {\n" +
                "                        \"w\": 150,\n" +
                "                        \"resize\": \"crop\",\n" +
                "                        \"h\": 150\n" +
                "                    },\n" +
                "                    \"small\": {\n" +
                "                        \"w\": 340,\n" +
                "                        \"resize\": \"fit\",\n" +
                "                        \"h\": 255\n" +
                "                    },\n" +
                "                    \"large\": {\n" +
                "                        \"w\": 1024,\n" +
                "                        \"resize\": \"fit\",\n" +
                "                        \"h\": 768\n" +
                "                    },\n" +
                "                    \"medium\": {\n" +
                "                        \"w\": 600,\n" +
                "                        \"resize\": \"fit\",\n" +
                "                        \"h\": 450\n" +
                "                    }\n" +
                "                },\n" +
                "                \"id\": 574666182252458000,\n" +
                "                \"media_url_https\": \"https://pbs.twimg.com/media/B_mf2RmWcAApvbF.jpg\",\n" +
                "                \"media_url\": \"http://pbs.twimg.com/media/B_mf2RmWcAApvbF.jpg\",\n" +
                "                \"expanded_url\": \"http://twitter.com/Juanternero/status/574666182323793920/photo/1\",\n" +
                "                \"source_status_id_str\": \"574666182323793920\",\n" +
                "                \"indices\": [\n" +
                "                    124,\n" +
                "                    140\n" +
                "                ],\n" +
                "                \"source_status_id\": 574666182323793900,\n" +
                "                \"id_str\": \"574666182252457984\",\n" +
                "                \"type\": \"photo\",\n" +
                "                \"display_url\": \"pic.twitter.com/zFVWPsiqrW\",\n" +
                "                \"url\": \"http://t.co/zFVWPsiqrW\"\n" +
                "            }\n" +
                "        ],\n" +
                "        \"user_mentions\": [\n" +
                "            {\n" +
                "                \"id\": 291041963,\n" +
                "                \"name\": \"Juan Ternero\",\n" +
                "                \"indices\": [\n" +
                "                    3,\n" +
                "                    15\n" +
                "                ],\n" +
                "                \"screen_name\": \"Juanternero\",\n" +
                "                \"id_str\": \"291041963\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"id\": 19028805,\n" +
                "                \"name\": \"Ciudadanos \",\n" +
                "                \"indices\": [\n" +
                "                    94,\n" +
                "                    107\n" +
                "                ],\n" +
                "                \"screen_name\": \"CiudadanosCs\",\n" +
                "                \"id_str\": \"19028805\"\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"source\": \"<a href=\\\"http://twitter.com/download/android\\\" rel=\\\"nofollow\\\">Twitter for Android</a>\",\n" +
                "    \"favorited\": false,\n" +
                "    \"in_reply_to_user_id\": null,\n" +
                "    \"retweet_count\": 0,\n" +
                "    \"id_str\": \"574840856303300608\",\n" +
                "    \"user\": {\n" +
                "        \"location\": \"\",\n" +
                "        \"default_profile\": true,\n" +
                "        \"profile_background_tile\": false,\n" +
                "        \"statuses_count\": 6,\n" +
                "        \"lang\": \"es\",\n" +
                "        \"profile_link_color\": \"0084B4\",\n" +
                "        \"profile_banner_url\": \"https://pbs.twimg.com/profile_banners/305610681/1425854779\",\n" +
                "        \"id\": 305610681,\n" +
                "        \"following\": null,\n" +
                "        \"protected\": false,\n" +
                "        \"favourites_count\": 0,\n" +
                "        \"profile_text_color\": \"333333\",\n" +
                "        \"verified\": false,\n" +
                "        \"description\": null,\n" +
                "        \"contributors_enabled\": false,\n" +
                "        \"profile_sidebar_border_color\": \"C0DEED\",\n" +
                "        \"name\": \"Alexar12\",\n" +
                "        \"profile_background_color\": \"C0DEED\",\n" +
                "        \"created_at\": \"Thu May 26 14:20:35 +0000 2011\",\n" +
                "        \"default_profile_image\": false,\n" +
                "        \"followers_count\": 4,\n" +
                "        \"profile_image_url_https\": \"https://pbs.twimg.com/profile_images/574702867455565826/syo79lGm_normal.jpeg\",\n" +
                "        \"geo_enabled\": false,\n" +
                "        \"profile_background_image_url\": \"http://abs.twimg.com/images/themes/theme1/bg.png\",\n" +
                "        \"profile_background_image_url_https\": \"https://abs.twimg.com/images/themes/theme1/bg.png\",\n" +
                "        \"follow_request_sent\": null,\n" +
                "        \"url\": null,\n" +
                "        \"utc_offset\": null,\n" +
                "        \"time_zone\": null,\n" +
                "        \"notifications\": null,\n" +
                "        \"profile_use_background_image\": true,\n" +
                "        \"friends_count\": 11,\n" +
                "        \"profile_sidebar_fill_color\": \"DDEEF6\",\n" +
                "        \"screen_name\": \"thealexar12\",\n" +
                "        \"id_str\": \"305610681\",\n" +
                "        \"profile_image_url\": \"http://pbs.twimg.com/profile_images/574702867455565826/syo79lGm_normal.jpeg\",\n" +
                "        \"listed_count\": 0,\n" +
                "        \"is_translator\": false\n" +
                "    }\n" +
                "}";
        try {
            JSONObject jsonObject = new JSONObject(jsonString);
            iterateJSONObject(jsonObject, null, "");
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    static private Put iterateJSONArray(JSONArray jsonArray, Put put, String parent) throws JSONException {
        for (int i = 0; i < jsonArray.length(); i++) {
            String qualifier = parent + ":" + i;
            Object object = jsonArray.get(i);
            if (object instanceof JSONObject) {
                iterateJSONObject((JSONObject) object, put, qualifier);
            } else if (object instanceof  JSONArray) {
                iterateJSONArray((JSONArray) object, put, qualifier);
            } else {
                String value = String.valueOf(object);
                //put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                System.out.println(String.format("data - %s - %s", qualifier, value));
            }
        }
        return put;
    }

    static private Put iterateJSONObject(JSONObject jsonObject, Put put, String parent) throws JSONException {
        Iterator keys = jsonObject.keys();
        while(keys.hasNext()) {
            String key = (String) keys.next();
            String qualifier = key;
            if (!parent.equals("")) {
                qualifier = parent + ":" + key;
            }
            Object object = jsonObject.get(key);
            if (object instanceof JSONObject) {
                iterateJSONObject((JSONObject) object, put, qualifier);
            } else if (object instanceof JSONArray) {
                    JSONArray jsonArray = (JSONArray) object;
                    for (int i = 0; i < jsonArray.length(); i++) {
                        Object arrayObject = jsonArray.get(i);
                        if (arrayObject instanceof JSONObject) {
                            iterateJSONObject((JSONObject) arrayObject, put, qualifier + ":" + i);
                        } else if (arrayObject instanceof JSONArray) {
                            iterateJSONArray((JSONArray) arrayObject, put, qualifier + ":" + i);
                        } else {
                            String value = String.valueOf(arrayObject);
                            //put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                            System.out.println(String.format("data - %s - %s", qualifier + ":" + i, value));
                        }
                    }
                }
            else {
                String value = String.valueOf(object);
                System.out.println(String.format("data - %s - %s", qualifier, value));
                //put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            }
        }
        return put;
    }
}
