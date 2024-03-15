from datetime import datetime, timedelta
from flatten_json import flatten

# @profile
def filterData(database):
    correctData = {}
    incorrectData = []
    required_keys = {
                        "REWARD_CREATED" : ['type', 'event_id', 'client', 'user_id', 'analytics_version','timestamp', 'event_name', 'campaign_details_campaign_id','campaign_details_campaign_name', 'campaign_details_campaign_experience','campaign_details_campaign_status', 'reward_details_key', 'reward_details_stepsCompleted','reward_details_activityId', 'reward_details_reward_coupon_code','reward_details_reward_index', 'reward_details_reward_id','reward_details_reward_status', 'reward_details_reward_title','reward_details_reward_body','reward_details_audiance_id', 'reward_details_reward_type','reward_details_reward_amount'],
                        "GAMECHALLENGE_ACTIVITY_COMPLETED": ['type', 'event_id', 'client', 'user_id', 'analytics_version', 'timestamp', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'campaign_details_campaign_activity_activity_completed_total', 'campaign_details_campaign_activity_activity_completed_daily', 'campaign_details_campaign_activity_activity_limits_total', 'campaign_details_campaign_activity_activity_limits_daily', 'campaign_details_campaign_activity_campaign_activity_status', 'campaign_details_campaign_activity_campaign_activity_id', 'campaign_details_campaign_activity_campaign_activity_event_name', 'campaign_details_campaign_activity_campaign_activity_completed_on', 'campaign_details_campaign_activity_activity_chances_credited', 'event_name'],
                        "CAMPAIGN_COMPLETED" : ['type', 'event_id', 'user_id', 'analytics_version', 'timestamp', 'client', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_state', 'reward_details_ruleId', 'reward_details_expiryDate', 'reward_details_reward_coupon_code', 'reward_details_reward_index', 'reward_details_reward_id', 'reward_details_reward_status', 'reward_details_reward_title', 'reward_details_score', 'reward_details_reward_body', 'reward_details_reward_expiry', 'reward_details_reward_type', 'reward_details_reward_amount', 'event_name'],
                        "CAMPAIGN_EXPIRED" : ['analytics_version', 'client', 'event_id', 'event_name', 'event_properties_campaign_details_campaign_experience', 'event_properties_campaign_details_campaign_expiration_type', 'event_properties_campaign_details_campaign_expired', 'event_properties_campaign_details_campaign_expiry', 'event_properties_campaign_details_campaign_id', 'event_properties_campaign_details_campaign_name', 'event_properties_campaign_details_campaign_state', 'timestamp', 'user_id'],
                        "CAMPAIGN_JOINED" : ['type', 'event_id', 'user_id', 'analytics_version', 'timestamp', 'client', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'event_name'],
                        "MULTISTEP_ACTIVITY_COMPLETED" : ['type', 'event_id', 'client', 'user_id', 'analytics_version', 'timestamp', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'campaign_details_campaign_activity_activity_completed_total', 'campaign_details_campaign_activity_activity_completed_daily', 'campaign_details_campaign_activity_activity_limits_total', 'campaign_details_campaign_activity_activity_limits_daily', 'campaign_details_campaign_activity_campaign_activity_status', 'campaign_details_campaign_activity_campaign_activity_id', 'campaign_details_campaign_activity_campaign_activity_event_name', 'campaign_details_campaign_activity_campaign_activity_completed_on', 'event_name'],
                        "REWARD_GRANTED" : ['type', 'event_id', 'client', 'user_id', 'analytics_version', 'timestamp', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status','reward_details_key', 'reward_details_stepsCompleted', 'reward_details_transaction_id', 'reward_details_activityId', 'reward_details_reward_coupon_code', 'reward_details_reward_index', 'reward_details_reward_id', 'reward_details_reward_status', 'reward_details_reward_title', 'reward_details_reward_body', 'reward_details_reward_type', 'reward_details_reward_amount', 'event_name'],
                        "STREAK_ACTIVITY_COMPLETED" : ['type', 'event_id', 'client', 'user_id', 'analytics_version', 'timestamp', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'campaign_details_campaign_activity_activity_completed_total', 'campaign_details_campaign_activity_activity_completed_daily', 'campaign_details_campaign_activity_activity_limits_total', 'campaign_details_campaign_activity_activity_limits_daily', 'campaign_details_campaign_activity_campaign_activity_status', 'campaign_details_campaign_activity_campaign_activity_id', 'campaign_details_campaign_activity_campaign_activity_event_name', 'campaign_details_campaign_activity_campaign_activity_completed_on', 'event_name'],
                        "ALL_REWARDS_CONSUMED": ['type', 'event_id', 'user_id', 'analytics_version', 'timestamp', 'client', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'event_name'],
                        "ENTRY_POINT_DISMISS" : ['analytics_version', 'timestamp', 'event_id', 'user_id', 'event_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_sdk_version', 'entry_point_data_entry_point_id', 'entry_point_data_entry_point_name', 'entry_point_data_entry_point_location', 'entry_point_data_entry_point_content_type', 'entry_point_data_entry_point_content_campaign_id', 'entry_point_data_entry_point_content_static_url', 'entry_point_data_entry_point_container', 'entry_point_data_entry_point_action', 'campaign_id', 'client', 'userId', 'eventId', 'headers'],
                        "PAGE_OPENED" : ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'client', 'userId', 'eventId', 'headers', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'campaign_details_reward_amount'],
                        "BUTTON_CLICKED": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_reward_amount', 'optional_payload_stepsCompleted', 'optional_payload_activityId', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_button_name', 'interaction_details_properties_channel', 'interaction_details_properties_code', 'interaction_details_properties_deep_link', 'interaction_details_properties_web_link', 'client', 'userId', 'eventId', 'headers'],
                        "BACK_BUTTON_CLICKED": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details', 'client', 'userId', 'eventId', 'headers', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_state'],
                        "ENTRY_POINT_LOAD" : ['analytics_version', 'timestamp', 'event_id', 'user_id', 'event_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_sdk_version', 'entry_point_data_entry_point_id', 'entry_point_data_entry_point_name', 'entry_point_data_entry_point_location', 'entry_point_data_entry_point_content_type', 'entry_point_data_entry_point_content_campaign_id', 'entry_point_data_entry_point_content_static_url', 'entry_point_data_entry_point_container', 'entry_point_data_entry_point_action_action_type', 'entry_point_data_entry_point_action_open_container', 'entry_point_data_entry_point_action_open_content_type', 'entry_point_data_entry_point_action_open_content_campaign_id', 'entry_point_data_entry_point_action_open_content_static_url', 'campaign_id', 'client', 'userId', 'eventId', 'headers'],
                        "GAME_PLAYED" : ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'campaign_details_campaign_state', 'campaign_details_reward_amount', 'platform_details_device_type', 'platform_details_os', 'optional_payload_stepsCompleted', 'optional_payload_activityId','optional_payload_gratification_id','platform_details_agent_type', 'platform_details_app_platform', 'client', 'userId', 'eventId', 'headers'],
                        "PROMPT_SHOWN": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'client', 'userId', 'eventId', 'headers'],
                        "SURVEY_ANSWERED": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_survey_details_survey_page_index', 'interaction_details_survey_details_all_fields_0_field_type', 'interaction_details_survey_details_all_fields_0_survey_question_index', 'interaction_details_survey_details_all_fields_0_question', 'interaction_details_survey_details_all_fields_0_user_answers_0', 'interaction_details_survey_details_formId', 'interaction_details_survey_details_formSectionId', 'client', 'userId', 'eventId', 'headers'],
                        "ACTIVITY_CLICKED": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_state', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_button_name', 'client', 'userId', 'eventId', 'headers'],
                        "VIEW_REWARD_CLICKED": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_button_name', 'client', 'userId', 'eventId', 'headers'],
                        "VIEW_ALL" : ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_button_name', 'client', 'userId', 'eventId', 'headers'],
                        "COUPON_CODE_COPIED": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_properties_channel', 'interaction_details_properties_code', 'interaction_details_properties_deep_link', 'interaction_details_properties_web_link', 'client', 'userId', 'eventId', 'headers'],
                        "CAMPAIGN_PLAY" : ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform','optional_payload_gratification_id', 'interaction_details', 'client', 'userId', 'eventId', 'headers'],
                        "ENTRY_POINT_CLICK": ['analytics_version', 'timestamp', 'event_id', 'user_id', 'event_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_sdk_version', 'entry_point_data_entry_point_id', 'entry_point_data_entry_point_name', 'entry_point_data_entry_point_location', 'entry_point_data_entry_point_content_type', 'entry_point_data_entry_point_content_campaign_id', 'entry_point_data_entry_point_content_static_url', 'entry_point_data_entry_point_container', 'entry_point_data_entry_point_action_action_type', 'entry_point_data_entry_point_action_open_container', 'entry_point_data_entry_point_action_open_content_type', 'entry_point_data_entry_point_action_open_content_campaign_id', 'entry_point_data_entry_point_action_open_content_static_url', 'campaign_id', 'client', 'userId', 'eventId', 'headers'],
                        "CAMPAIGN_BANNER_CLICKED" : ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details', 'client', 'userId', 'eventId', 'headers'],
                        "WEBVIEW_DISMISS": ['event_id', 'webview_content_absolute_height', 'webview_content_campaignId', 'webview_content_webview_layout', 'webview_content_relative_height', 'webview_content_webview_url', 'user_id', 'event_name', 'dismiss_trigger', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "WEBVIEW_LOAD": ['event_id', 'webview_content_absolute_height', 'webview_content_campaignId', 'webview_content_webview_layout', 'webview_content_relative_height', 'webview_content_webview_url', 'user_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "UI_INTERACTION" : ['timestamp', 'event_name', 'event_properties', 'client', 'userId', 'eventId', 'headers'],
                        "QUIZ_QUESTION_ANSWERED" : ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_quiz_details_session_id', 'interaction_details_quiz_details_question_index', 'interaction_details_quiz_details_user_answer', 'interaction_details_quiz_details_correct_answer', 'interaction_details_quiz_details_is_correct', 'client', 'userId', 'eventId', 'headers'],
                        "wallet_interaction": ['timestamp', 'event_name', 'event_properties_pageName', 'event_properties_action', 'client', 'userId', 'eventId', 'headers'],
                        "PIP_ENTRY_POINT_CTA_CLICK": ['entry_point_data_entry_point_id', 'entry_point_data_entry_point_location', 'entry_point_data_entry_point_is_expanded', 'entry_point_data_entry_point_container', 'entry_point_data_entry_point_name', 'event_id', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "MUTE_PIP_VIDEO": ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "PIP_ENTRY_POINT_CLICK": ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "PIP_ENTRY_POINT_LOAD": ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "PIP_VIDEO_75_COMPLETED" : ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "COLLAPSE_PIP_VIDEO": ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "UNMUTE_PIP_VIDEO": ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "PIP_VIDEO_COMPLETED": ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "PIP_VIDEO_25_COMPLETED": ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "PIP_VIDEO_50_COMPLETED" : ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "PIP_ENTRY_POINT_DISMISS": ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers'],
                        "PIP_ENTRY_POINT_CLICK" : ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                    }

    for collection_name in database.list_collection_names():
        current_time = datetime.now()
        one_hour_ago = current_time - timedelta(hours=1)
        hour = one_hour_ago.hour

        if collection_name.startswith(str(10)):

            collection = database[collection_name]
            data = list(collection.find({}, {"_id": 0}))

            flattened_data = [];

            for data_dict in data:

                if(data_dict["event_name"] == "PIP_ENTRY_POINT_CLICK"):
                    if(data_dict.get("webview_content") is None or data_dict.get("webview_content") == {}):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "PIP_ENTRY_POINT_DISMISS"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "PIP_ENTRY_POINT_LOAD"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "PIP_VIDEO_50_COMPLETED"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "PIP_VIDEO_25_COMPLETED"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "PIP_VIDEO_COMPLETED"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "UNMUTE_PIP_VIDEO" ):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "COLLAPSE_PIP_VIDEO"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None
                
                if(data_dict["event_name"] == "PIP_VIDEO_75_COMPLETED"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "PIP_ENTRY_POINT_LOAD"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "PIP_ENTRY_POINT_CLICK"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "MUTE_PIP_VIDEO"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "PIP_ENTRY_POINT_CTA_CLICK"):
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_location": None,
                                                            "entry_point_is_expanded": None,
                                                            "entry_point_container": None,
                                                            "entry_point_name": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("entry_point_id") is None):
                            data_dict["webview_content"]["entry_point_id"] = None
                        if(data_dict["webview_content"].get("entry_point_location") is None):
                            data_dict["webview_content"]["entry_point_location"] = None
                        if(data_dict["webview_content"].get("entry_point_is_expanded") is None):
                            data_dict["webview_content"]["entry_point_is_expanded"] = None
                        if(data_dict["webview_content"].get("entry_point_container") is None):
                            data_dict["webview_content"]["entry_point_container"] = None
                        if(data_dict["webview_content"].get("entry_point_name") is None):
                            data_dict["webview_content"]["entry_point_name"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict["event_name"] == "WEBVIEW_LOAD"):
                    if(data_dict.get("campaign_id") is None):
                        data_dict["campaign_id"] = None
                    
                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None
                    
                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "absolute_height": None,
                                                            "campaignId": None,
                                                            "webview_layout": None,
                                                            "relative_height": None,
                                                            "webview_url": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("absolute_height") is None):
                            data_dict["webview_content"]["absolute_height"] = None
                        if(data_dict["webview_content"].get("campaignId") is None):
                            data_dict["webview_content"]["campaignId"] = None
                        if(data_dict["webview_content"].get("webview_layout") is None):
                            data_dict["webview_content"]["webview_layout"] = None
                        if(data_dict["webview_content"].get("relative_height") is None):
                            data_dict["webview_content"]["relative_height"] = None
                        if(data_dict["webview_content"].get("webview_url") is None):
                            data_dict["webview_content"]["webview_url"] = None

                if(data_dict["event_name"] == "WEBVIEW_DISMISS"):
                    if(data_dict.get("campaign_id") is None):
                        data_dict["campaign_id"] = None

                    if(data_dict.get("webview_content") is None):
                        data_dict["webview_content"] = {
                                                            "absolute_height": None,
                                                            "campaignId": None,
                                                            "webview_layout": None,
                                                            "relative_height": None,
                                                            "webview_url": None
                                                        }
                    else:
                        if(data_dict["webview_content"].get("absolute_height") is None):
                            data_dict["webview_content"]["absolute_height"] = None
                        if(data_dict["webview_content"].get("campaignId") is None):
                            data_dict["webview_content"]["campaignId"] = None
                        if(data_dict["webview_content"].get("webview_layout") is None):
                            data_dict["webview_content"]["webview_layout"] = None
                        if(data_dict["webview_content"].get("relative_height") is None):
                            data_dict["webview_content"]["relative_height"] = None
                        if(data_dict["webview_content"].get("webview_url") is None):
                            data_dict["webview_content"]["webview_url"] = None
                    
                    if(data_dict.get("dismiss_trigger") is None):
                        data_dict["dismiss_trigger"] = None

                    if(data_dict.get("platform_details") is None):
                        data_dict["platform_details"] = {
                                                            "app_platform": None,
                                                            "os": None,
                                                            "sdk_version": None,
                                                            "device_type": None
                                                        }
                    else:
                        if(data_dict["platform_details"].get("app_platform") is None):
                            data_dict["platform_details"]["app_platform"] = None
                        if(data_dict["platform_details"].get("os") is None):
                            data_dict["platform_details"]["os"] = None
                        if(data_dict["platform_details"].get("sdk_version") is None):
                            data_dict["platform_details"]["sdk_version"] = None
                        if(data_dict["platform_details"].get("device_type") is None):
                            data_dict["platform_details"]["device_type"] = None

                if(data_dict.get("event_name") == "ENTRY_POINT_LOAD"):
                    if(data_dict.get("entry_point_data") is None):
                        data_dict["entry_point_data"] = {
                                                            "entry_point_id": None,
                                                            "entry_point_name": None,
                                                            "entry_point_location": None,
                                                            "entry_point_content_type": None,
                                                            "entry_point_content_campaign_id": None,
                                                            "entry_point_content_static_url": None,
                                                            "entry_point_container": None,
                                                            "entry_point_action": {
                                                                "action_type": None,
                                                                "open_container": None,
                                                                "open_content" : {
                                                                    "type": None,
                                                                    "campaign_id": None,
                                                                    "static_url": None
                                                                }
                                                            }
                                                        }
                    else:
                        if(data_dict["entry_point_data"].get("entry_point_id") is None):
                            data_dict["entry_point_data"]["entry_point_id"] = None
                        if(data_dict["entry_point_data"].get("entry_point_name") is None):
                            data_dict["entry_point_data"]["entry_point_name"] = None
                        if(data_dict["entry_point_data"].get("entry_point_location") is None):
                            data_dict["entry_point_data"]["entry_point_location"] = None
                        if(data_dict["entry_point_data"].get("entry_point_content_type") is None):
                            data_dict["entry_point_data"]["entry_point_content_type"] = None
                        if(data_dict["entry_point_data"].get("entry_point_content_campaign_id") is None):
                            data_dict["entry_point_data"]["entry_point_content_campaign_id"] = None
                        if(data_dict["entry_point_data"].get("entry_point_content_static_url") is None):
                            data_dict["entry_point_data"]["entry_point_content_static_url"] = None
                        if(data_dict["entry_point_data"].get("entry_point_container") is None):
                            data_dict["entry_point_data"]["entry_point_container"] = None
                        if(data_dict["entry_point_data"].get("entry_point_action") is None):
                            data_dict["entry_point_data"]["entry_point_action"] = {
                                                                                        "action_type": None,
                                                                                        "open_container": None,
                                                                                        "open_content" : {
                                                                                            "type": None,
                                                                                            "campaign_id": None,
                                                                                            "static_url": None
                                                                                        }
                                                                                    }
                        else:
                            if(data_dict["entry_point_data"]["entry_point_action"].get("action_type") is None):
                                data_dict["entry_point_data"]["entry_point_action"]["action_type"] = None
                            if(data_dict["entry_point_data"]["entry_point_action"].get("open_container") is None):
                                data_dict["entry_point_data"]["entry_point_action"]["open_container"] = None
                            if(data_dict["entry_point_data"]["entry_point_action"].get("open_content") is None):
                                data_dict["entry_point_data"]["entry_point_action"]["open_content"] = {
                                                                                                        "type": None,
                                                                                                        "campaign_id": None,
                                                                                                        "static_url": None
                                                                                                    }
                            else:
                                if(data_dict["entry_point_data"]["entry_point_action"]["open_content"].get("type") is None):
                                    data_dict["entry_point_data"]["entry_point_action"]["open_content"]["type"] = None
                                if(data_dict["entry_point_data"]["entry_point_action"]["open_content"].get("campaign_id") is None):
                                    data_dict["entry_point_data"]["entry_point_action"]["open_content"]["campaign_id"] = None
                                if(data_dict["entry_point_data"]["entry_point_action"]["open_content"].get("static_url") is None):
                                    data_dict["entry_point_data"]["entry_point_action"]["open_content"]["static_url"] = None

                if(data_dict["event_name"] == "CAMPAIGN_PLAY"):
                    if(data_dict.get("optional_payload") is None):
                        data_dict["optional_payload"] = {
                                                            "gratification_id": None
                                                        }

                if(data_dict["event_name"] ==  "PAGE_OPENED"):
                    if(data_dict.get("optional_payload") is not None):
                        data_dict.pop('optional_payload')
                    if(data_dict.get("campaign_details") is None):
                        data_dict["campaign_details"] = {
                                                            "campaign_id": None,
                                                            "campaign_name": None,
                                                            "campaign_experience": None,
                                                            "reward_user_id": None,
                                                            "reward_status": None,
                                                            "campaign_state": None,
                                                            "selected_slot_index": None,
                                                            "reward_type": None,
                                                            "reward_title": None,
                                                            "coupon_code": None,
                                                            "reward_amount": None
                                                        }
                    else:
                        if(data_dict["campaign_details"].get("campaign_id") is None):
                            data_dict["campaign_details"]["campaign_id"] = None
                        if(data_dict["campaign_details"].get("campaign_name") is None):
                            data_dict["campaign_details"]["campaign_name"] = None
                        if(data_dict["campaign_details"].get("campaign_experience") is None):
                            data_dict["campaign_details"]["campaign_experience"] = None
                        if(data_dict["campaign_details"].get("reward_user_id") is None):
                            data_dict["campaign_details"]["reward_user_id"] = None
                        if(data_dict["campaign_details"].get("reward_status") is None):
                            data_dict["campaign_details"]["reward_status"] = None
                        if(data_dict["campaign_details"].get("campaign_state") is None):
                            data_dict["campaign_details"]["campaign_state"] = None
                        if(data_dict["campaign_details"].get("selected_slot_index") is None):
                            data_dict["campaign_details"]["selected_slot_index"] = None
                        if(data_dict["campaign_details"].get("reward_type") is None):
                            data_dict["campaign_details"]["reward_type"] = None
                        if(data_dict["campaign_details"].get("reward_title") is None):
                            data_dict["campaign_details"]["reward_title"] = None
                        if(data_dict["campaign_details"].get("coupon_code") is None):
                            data_dict["campaign_details"]["coupon_code"] = None
                        if(data_dict["campaign_details"].get("reward_amount") is None):
                            data_dict["campaign_details"]["reward_amount"] = None

                if(data_dict.get("event_name") == "GAME_PLAYED"):
                    if(data_dict.get("optional_payload") is None):
                        data_dict["optional_payload"] = {
                                                            "stepsCompleted": None,
                                                            "activityId": None,
                                                            "gratification_id": None
                                                        }
                    else:
                        if(data_dict["optional_payload"].get("stepsCompleted") is None):
                            data_dict["optional_payload"]["stepsCompleted"] = None
                        if(data_dict["optional_payload"].get("activityId") is None):
                            data_dict["optional_payload"]["activityId"] = None
                        if(data_dict["optional_payload"].get("gratification_id") is None):
                            data_dict["optional_payload"]["gratification_id"] = None

                    if(data_dict["campaign_details"].get("reward_amount") is None):
                        data_dict["campaign_details"]["reward_amount"] = None
                    if(data_dict["campaign_details"].get("campaign_name") is None):
                        data_dict["campaign_details"]["campaign_name"] = None

                if(data_dict.get("event_name") == "BUTTON_CLICKED"):
                    if(data_dict.get("interaction_details") is None):
                        data_dict["interaction_details"] = {
                                                            "button_name": None,
                                                            "properties_channel": None,
                                                            "properties_code": None,
                                                            "properties_deep_link": None,
                                                            "properties_web_link": None
                                                        }
                    else:
                        if(data_dict["interaction_details"].get("button_name") is None):
                            data_dict["interaction_details"]["button_name"] = None
                        if(data_dict["interaction_details"].get("properties_channel") is None):
                            data_dict["interaction_details"]["properties_channel"] = None
                        if(data_dict["interaction_details"].get("properties_code") is None):
                            data_dict["interaction_details"]["properties_code"] = None
                        if(data_dict["interaction_details"].get("properties_deep_link") is None):
                            data_dict["interaction_details"]["properties_deep_link"] = None
                        if(data_dict["interaction_details"].get("properties_web_link") is None):
                            data_dict["interaction_details"]["properties_web_link"] = None
                    if(data_dict.get("optional_payload") is None):
                        data_dict["optional_payload"] = {
                                                            "stepsCompleted": None,
                                                            "activityId": None
                                                        }
                    else:
                        if(data_dict["optional_payload"].get("stepsCompleted") is None):
                            data_dict["optional_payload"]["stepsCompleted"] = None
                        if(data_dict["optional_payload"].get("activityId") is None):
                            data_dict["optional_payload"]["activityId"] = None
                    if(data_dict.get("campaign_details") is None):
                        data_dict["campaign_details"] = {
                                                            "campaign_id": None,
                                                            "campaign_name": None,
                                                            "campaign_experience": None,
                                                            "reward_user_id": None,
                                                            "reward_status": None,
                                                            "campaign_state": None,
                                                            "selected_slot_index": None,
                                                            "reward_type": None,
                                                            "reward_title": None,
                                                            "coupon_code": None,
                                                            "reward_amount": None
                                                        }
                    else:
                        if(data_dict["campaign_details"].get("campaign_id") is None):
                            data_dict["campaign_details"]["campaign_id"] = None
                        if(data_dict["campaign_details"].get("campaign_name") is None):
                            data_dict["campaign_details"]["campaign_name"] = None
                        if(data_dict["campaign_details"].get("campaign_experience") is None):
                            data_dict["campaign_details"]["campaign_experience"] = None
                        if(data_dict["campaign_details"].get("reward_user_id") is None):
                            data_dict["campaign_details"]["reward_user_id"] = None
                        if(data_dict["campaign_details"].get("reward_status") is None):
                            data_dict["campaign_details"]["reward_status"] = None
                        if(data_dict["campaign_details"].get("campaign_state") is None):
                            data_dict["campaign_details"]["campaign_state"] = None
                        if(data_dict["campaign_details"].get("selected_slot_index") is None):
                            data_dict["campaign_details"]["selected_slot_index"] = None
                        if(data_dict["campaign_details"].get("reward_type") is None):
                            data_dict["campaign_details"]["reward_type"] = None
                        if(data_dict["campaign_details"].get("reward_title") is None):
                            data_dict["campaign_details"]["reward_title"] = None
                        if(data_dict["campaign_details"].get("coupon_code") is None):
                            data_dict["campaign_details"]["coupon_code"] = None
                        if(data_dict["campaign_details"].get("reward_amount") is None):
                            data_dict["campaign_details"]["reward_amount"] = None

                if(data_dict["event_name"] == "BACK_BUTTON_CLICKED"):
                    if(data_dict.get("campaign_details") is None):
                        data_dict["campaign_details"] = {
                                                            "campaign_id": None,
                                                            "campaign_name": None,
                                                            "campaign_experience": None,
                                                            "campaign_state": None,
                                                        }
                    else:
                        if(data_dict["campaign_details"].get("campaign_id") is None):
                            data_dict["campaign_details"]["campaign_id"] = None
                        if(data_dict["campaign_details"].get("campaign_name") is None):
                            data_dict["campaign_details"]["campaign_name"] = None
                        if(data_dict["campaign_details"].get("campaign_experience") is None):
                            data_dict["campaign_details"]["campaign_experience"] = None
                        if(data_dict["campaign_details"].get("campaign_state") is None):
                            data_dict["campaign_details"]["campaign_state"] = None
                    
                if(data_dict["event_name"] == "REWARD_CREATED"):
                    if(data_dict["reward_details"].get("audiance_id") is None):
                        data_dict["reward_details"]["audiance_id"] = None
                    if(data_dict["reward_details"].get("key") is None):
                        data_dict["reward_details"]["key"] = None
                       
                if(data_dict["event_name"] == "REWARD_GRANTED"):
                    if(data_dict["reward_details"].get("transaction_id") is None):
                        data_dict["reward_details"]["transaction_id"] = None
                    if(data_dict["reward_details"].get("key") is None):
                        data_dict["reward_details"]["key"] = None

                if(data_dict["event_name"] == "CAMPAIGN_COMPLETED"):
                    if(data_dict.get("reward_details") is None):
                        data_dict["reward_details"] =  {
                                                            "score": None,
                                                            "ruleId": None,
                                                            "expiryDate": None,
                                                            "reward_coupon_code": None,
                                                            "reward_index": None,
                                                            "reward_id": None,
                                                            "reward_status": None,
                                                            "reward_title": None,
                                                            "reward_body": None,
                                                            "reward_expiry": None,
                                                            "reward_type": None,
                                                            "reward_amount": None
                                                        }
                    else: 
                        if(data_dict["reward_details"].get("score") is None):
                            data_dict["reward_details"]["score"] = None
                        if(data_dict["reward_details"].get("ruleId") is None):
                            data_dict["reward_details"]["ruleId"] = None
                        if(data_dict["reward_details"].get("expiryDate") is None):
                            data_dict["reward_details"]["expiryDate"] = None

                if(data_dict["event_name"] == "UI_INTERACTION"):
                    flattened_data.append(data_dict)
                else:
                    flattened_data.append(flatten(data_dict))

            if required_keys.get(collection_name.split("_",1)[1]) is not None:

                correctData[collection_name.split("_",1)[1]] = []
                required_keys_set = set(required_keys[collection_name.split("_", 1)[1]])
                for item in flattened_data:
                    item_keys_set = set(key.strip() for key in item.keys())

                    if item_keys_set == required_keys_set:
                        correctData[collection_name.split("_", 1)[1]].append(item)
                    else:
                        incorrectData.append(item)

            else:
                for item in flattened_data:
                    incorrectData.append(item)

    return [correctData, incorrectData]