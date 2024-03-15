import pandas as pd
from clickhouse_driver import Client
from pprint import pprint

# @profile
def storeDataIntoClickHouse(data, host, password, user, database_name):
    if(data == {}):
        return True
    
    try:

        create_table_query = ""
        client = Client(host=host, password=password, user=user, database=database_name)
        for(key, value) in data.items():
            df = pd.DataFrame(value)
            if(df is None or df.empty):
                continue

            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', utc=True)
            else:
                raise ValueError("DataFrame does not contain 'timestamp' column")
            
            # unique_types = {}

            # # Iterate over each column
            # for column in df.columns:
            #     unique_types[column] = []

            # # Iterate over each row
            # for _, row in df.iterrows():
            #     # Iterate over each element in the row
            #     for column, value in row.items():
            #         value_type = type(value)
            #         # Add the type to the list of unique types for the column if not already present
            #         if value_type not in unique_types[column]:
            #             unique_types[column].append(value_type)

            # print(key)
            # pprint(unique_types)

            if(key == "REWARD_CREATED"):
                create_table_query = f'''
                        CREATE TABLE IF NOT EXISTS {key} (
                        type Nullable(String),
                        event_id Nullable(String),
                        client Nullable(String),
                        user_id Nullable(String),
                        analytics_version Nullable(String),
                        timestamp DateTime,
                        event_name Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_status Nullable(String),
                        reward_details_stepsCompleted Nullable(Int64),
                        reward_details_activityId Nullable(String),
                        reward_details_key Nullable(String),
                        reward_details_reward_coupon_code Nullable(String),
                        reward_details_reward_index Nullable(Int64),
                        reward_details_reward_id Nullable(String),
                        reward_details_reward_status Nullable(String),
                        reward_details_reward_title Nullable(String),
                        reward_details_reward_body Nullable(String),
                        reward_details_audiance_id Nullable(String),
                        reward_details_reward_type Nullable(String),
                        reward_details_reward_amount Nullable(Float64)
                    ) ENGINE = MergeTree()
                    ORDER BY timestamp;
                '''
                df['reward_details_reward_index'] = df['reward_details_reward_index'].astype(int, errors='ignore').fillna(0)
                df['reward_details_reward_index'] = df['reward_details_reward_index'].fillna(0)
                df['reward_details_reward_amount'] = df['reward_details_reward_amount'].astype(float, errors='ignore').fillna(0)
                df['reward_details_reward_amount'] = df['reward_details_reward_amount'].fillna(0)
                df['reward_details_stepsCompleted'] = df['reward_details_stepsCompleted'].astype(int, errors='ignore').fillna(0)
                df['reward_details_stepsCompleted'] = df['reward_details_stepsCompleted'].fillna(0)

            elif(key == "GAMECHALLENGE_ACTIVITY_COMPLETED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        type Nullable(String),
                        event_id Nullable(String),
                        client Nullable(String),
                        user_id Nullable(String),
                        analytics_version Nullable(String),
                        timestamp DateTime,
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_status Nullable(String),
                        campaign_details_campaign_steps_completed Nullable(Int64),
                        campaign_details_campaign_total_steps Nullable(Int64),
                        campaign_details_campaign_expires_on Nullable(String),
                        campaign_details_campaign_expiry_type Nullable(String),
                        campaign_details_campaign_expired Nullable(UInt8),
                        campaign_details_campaign_activity_activity_completed_total Nullable(Int64),
                        campaign_details_campaign_activity_activity_completed_daily Nullable(Int64),
                        campaign_details_campaign_activity_activity_limits_total Nullable(Int64),
                        campaign_details_campaign_activity_activity_limits_daily Nullable(Int64),
                        campaign_details_campaign_activity_campaign_activity_status Nullable(String),
                        campaign_details_campaign_activity_campaign_activity_id Nullable(String),
                        campaign_details_campaign_activity_campaign_activity_event_name Nullable(String),
                        campaign_details_campaign_activity_campaign_activity_completed_on Nullable(String),
                        campaign_details_campaign_activity_activity_chances_credited Nullable(Int64),
                        event_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY timestamp;
                '''
                df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_completed_total'] = df['campaign_details_campaign_activity_activity_completed_total'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_completed_daily'] = df['campaign_details_campaign_activity_activity_completed_daily'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_limits_total'] = df['campaign_details_campaign_activity_activity_limits_total'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_limits_daily'] = df['campaign_details_campaign_activity_activity_limits_daily'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_chances_credited'] = df['campaign_details_campaign_activity_activity_chances_credited'].astype(int, errors='ignore').fillna(0)
            
            elif(key == "CAMPAIGN_COMPLETED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        type Nullable(String),
                        event_id Nullable(String),
                        user_id Nullable(String),
                        analytics_version Nullable(String),
                        timestamp DateTime,
                        client Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        reward_details_ruleId Nullable(String),
                        reward_details_expiryDate Nullable(String),
                        reward_details_reward_coupon_code Nullable(String),
                        reward_details_score Nullable(Float64),
                        reward_details_reward_index Nullable(Float64),
                        reward_details_reward_id Nullable(String),
                        reward_details_reward_status Nullable(String),
                        reward_details_reward_title Nullable(String),
                        reward_details_reward_body Nullable(String),
                        reward_details_reward_expiry Nullable(String),
                        reward_details_reward_type Nullable(String),
                        reward_details_reward_amount Nullable(Float64),
                        event_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY timestamp;
                '''
                df['reward_details_score'] = df['reward_details_score'].astype(float, errors='ignore').fillna(0)
                df['reward_details_reward_index'] = df['reward_details_reward_index'].astype(float, errors='ignore').fillna(0)
                df['reward_details_reward_amount'] = df['reward_details_reward_amount'].astype(float, errors='ignore').fillna(0)
            
            elif( key == "CAMPAIGN_EXPIRED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        client Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        event_properties_campaign_details_campaign_experience Nullable(String),
                        event_properties_campaign_details_campaign_expiration_type Nullable(String),
                        event_properties_campaign_details_campaign_expired Nullable(UInt8),
                        event_properties_campaign_details_campaign_expiry Nullable(String),
                        event_properties_campaign_details_campaign_id Nullable(String),
                        event_properties_campaign_details_campaign_name Nullable(String),
                        event_properties_campaign_details_campaign_state Nullable(String),
                        timestamp DateTime,
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY timestamp;
                '''
                df['event_properties_campaign_details_campaign_expired'] = df['event_properties_campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)

            elif(key == "CAMPAIGN_JOINED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        type Nullable(String),
                        event_id Nullable(String),
                        user_id Nullable(String),
                        analytics_version Nullable(String),
                        timestamp DateTime,
                        client Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_status Nullable(String),
                        campaign_details_campaign_steps_completed Nullable(Int64),
                        campaign_details_campaign_total_steps Nullable(Int64),
                        campaign_details_campaign_expires_on Nullable(String),
                        campaign_details_campaign_expiry_type Nullable(String),
                        campaign_details_campaign_expired Nullable(UInt8),
                        event_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY timestamp;
                '''
                df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)

            elif(key == "MULTISTEP_ACTIVITY_COMPLETED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        type Nullable(String),
                        event_id Nullable(String),
                        client Nullable(String),
                        user_id Nullable(String),
                        analytics_version Nullable(String),
                        timestamp DateTime,
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_status Nullable(String),
                        campaign_details_campaign_steps_completed Nullable(Int64),
                        campaign_details_campaign_total_steps Nullable(Int64),
                        campaign_details_campaign_expires_on Nullable(String),
                        campaign_details_campaign_expiry_type Nullable(String),
                        campaign_details_campaign_expired Nullable(UInt8),
                        campaign_details_campaign_activity_activity_completed_total Nullable(Int64),
                        campaign_details_campaign_activity_activity_completed_daily Nullable(Int64),
                        campaign_details_campaign_activity_activity_limits_total Nullable(Int64),
                        campaign_details_campaign_activity_activity_limits_daily Nullable(Int64),
                        campaign_details_campaign_activity_campaign_activity_status Nullable(String),
                        campaign_details_campaign_activity_campaign_activity_id Nullable(String),
                        campaign_details_campaign_activity_campaign_activity_event_name Nullable(String),
                        campaign_details_campaign_activity_campaign_activity_completed_on Nullable(String),
                        event_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY timestamp;
                '''
                df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_completed_total'] = df['campaign_details_campaign_activity_activity_completed_total'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_completed_daily'] = df['campaign_details_campaign_activity_activity_completed_daily'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_limits_total'] = df['campaign_details_campaign_activity_activity_limits_total'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_limits_daily'] = df['campaign_details_campaign_activity_activity_limits_daily'].astype(int, errors='ignore').fillna(0)

            elif(key == "REWARD_GRANTED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        type Nullable(String),
                        event_id Nullable(String),
                        client Nullable(String),
                        user_id Nullable(String),
                        analytics_version Nullable(String),
                        timestamp DateTime,
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_status Nullable(String),
                        reward_details_stepsCompleted Nullable(Int64),
                        reward_details_transaction_id Nullable(String),
                        reward_details_activityId Nullable(String),
                        reward_details_key Nullable(String),
                        reward_details_reward_coupon_code Nullable(String),
                        reward_details_reward_index Nullable(Int64),
                        reward_details_reward_id Nullable(String),
                        reward_details_reward_status Nullable(String),
                        reward_details_reward_title Nullable(String),
                        reward_details_reward_body Nullable(String),
                        reward_details_reward_type Nullable(String),
                        reward_details_reward_amount Nullable(Float64),
                        event_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY timestamp;
                '''
                df['reward_details_reward_amount'] = df['reward_details_reward_amount'].astype(float, errors='ignore').fillna(0)
                df['reward_details_reward_index'] = df['reward_details_reward_index'].astype(int, errors='ignore').fillna(0)
                df['reward_details_stepsCompleted'] = df['reward_details_stepsCompleted'].astype(int, errors='ignore').fillna(0)
            
            elif(key == "ALL_REWARDS_CONSUMED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        type Nullable(String),
                        event_id Nullable(String),
                        user_id Nullable(String),
                        analytics_version Nullable(String),
                        timestamp DateTime,
                        client Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_status Nullable(String),
                        campaign_details_campaign_steps_completed Nullable(Int64), 
                        campaign_details_campaign_total_steps Nullable(Int64),
                        campaign_details_campaign_expires_on Nullable(String),
                        campaign_details_campaign_expiry_type Nullable(String),
                        campaign_details_campaign_expired Nullable(UInt8), 
                        event_name Nullable(String)
                    ) ENGINE = MergeTree() 
                    ORDER BY (timestamp);
                    '''
                df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)

            elif(key == "STREAK_ACTIVITY_COMPLETED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        type Nullable(String),
                        event_id Nullable(String),
                        client Nullable(String),
                        user_id Nullable(String),
                        analytics_version Nullable(String),
                        timestamp DateTime,
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_status Nullable(String),
                        campaign_details_campaign_steps_completed Nullable(Int64),
                        campaign_details_campaign_total_steps Nullable(Int64),
                        campaign_details_campaign_expires_on Nullable(String),
                        campaign_details_campaign_expiry_type Nullable(String),
                        campaign_details_campaign_expired Nullable(UInt8),
                        campaign_details_campaign_activity_activity_completed_total Nullable(Int64),
                        campaign_details_campaign_activity_activity_completed_daily Nullable(Int64),
                        campaign_details_campaign_activity_activity_limits_total Nullable(Int64),
                        campaign_details_campaign_activity_activity_limits_daily Nullable(Int64),
                        campaign_details_campaign_activity_campaign_activity_status Nullable(String),
                        campaign_details_campaign_activity_campaign_activity_id Nullable(String),
                        campaign_details_campaign_activity_campaign_activity_event_name Nullable(String),
                        campaign_details_campaign_activity_campaign_activity_completed_on Nullable(String),
                        event_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY timestamp;
                '''
                df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_completed_total'] = df['campaign_details_campaign_activity_activity_completed_total'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_completed_daily'] = df['campaign_details_campaign_activity_activity_completed_daily'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_limits_total'] = df['campaign_details_campaign_activity_activity_limits_total'].astype(int, errors='ignore').fillna(0)
                df['campaign_details_campaign_activity_activity_limits_daily'] = df['campaign_details_campaign_activity_activity_limits_daily'].astype(int, errors='ignore').fillna(0)

            elif(key == "ENTRY_POINT_DISMISS"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        entry_point_data_entry_point_action Nullable(String),
                        entry_point_data_entry_point_container Nullable(String),
                        entry_point_data_entry_point_content_campaign_id Nullable(String),
                        entry_point_data_entry_point_content_static_url Nullable(String),
                        entry_point_data_entry_point_content_type Nullable(String),
                        entry_point_data_entry_point_id Nullable(String),
                        entry_point_data_entry_point_location Nullable(String),
                        entry_point_data_entry_point_name Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        timestamp DateTime,
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "PAGE_OPENED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_amount Nullable(Float64),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        campaign_details_selected_slot_index Nullable(Float64),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

                df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(float, errors='ignore').fillna(0)
                df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(float, errors='ignore').fillna(0)
                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)

            elif(key == "BUTTON_CLICKED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_amount Nullable(Float64),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        campaign_details_selected_slot_index Nullable(Int64),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        interaction_details_button_name Nullable(String),
                        interaction_details_properties_channel Nullable(String),
                        interaction_details_properties_code Nullable(String),
                        interaction_details_properties_deep_link Nullable(String),
                        interaction_details_properties_web_link Nullable(String),
                        optional_payload_activityId Nullable(String),
                        optional_payload_stepsCompleted Nullable(Float64),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''
                df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(float, errors='ignore').fillna(0)
                df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
                df['optional_payload_stepsCompleted'] = df['optional_payload_stepsCompleted'].astype(float, errors='ignore').fillna(0)

            elif(key == "BACK_BUTTON_CLICKED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        interaction_details Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
                df['interaction_details'] = df['interaction_details'].astype(str, errors='ignore').fillna(0)

            elif(key == "ENTRY_POINT_LOAD"): 
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        entry_point_data_entry_point_action_action_type Nullable(String),
                        entry_point_data_entry_point_action_open_container Nullable(String),
                        entry_point_data_entry_point_action_open_content_campaign_id Nullable(String),
                        entry_point_data_entry_point_action_open_content_static_url Nullable(String),
                        entry_point_data_entry_point_action_open_content_type Nullable(String),
                        entry_point_data_entry_point_container Nullable(String),
                        entry_point_data_entry_point_content_campaign_id Nullable(String),
                        entry_point_data_entry_point_content_static_url Nullable(String),
                        entry_point_data_entry_point_content_type Nullable(String),
                        entry_point_data_entry_point_id Nullable(String),
                        entry_point_data_entry_point_location Nullable(String),
                        entry_point_data_entry_point_name Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        timestamp DateTime,
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "GAME_PLAYED"): 
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_amount Nullable(Float64),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        campaign_details_selected_slot_index Nullable(Int64),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        optional_payload_activityId Nullable(String),
                        optional_payload_gratification_id Nullable(String),
                        optional_payload_stepsCompleted Nullable(Float64),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''
                df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(float, errors='ignore').fillna(0)
                df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
                df['optional_payload_stepsCompleted'] = df['optional_payload_stepsCompleted'].astype(float, errors='ignore').fillna(0)
                df['optional_payload_stepsCompleted'] = df['optional_payload_stepsCompleted'].fillna(0)

            elif(key == "PROMPT_SHOWN"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        campaign_details_selected_slot_index Nullable(Int64),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

                df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)

            elif(key == "SURVEY_ANSWERED"):
                create_table_query= f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        campaign_details_selected_slot_index Nullable(Int64),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

                df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)

            elif(key == "ACTIVITY_CLICKED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        interaction_details_button_name Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)

            elif(key == "VIEW_REWARD_CLICKED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        campaign_details_selected_slot_index Nullable(Int64),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        interaction_details_button_name Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "VIEW_ALL"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        interaction_details_button_name Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''
            
            elif(key == "COUPON_CODE_COPIED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        campaign_details_selected_slot_index Nullable(Int64),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        interaction_details_properties_channel Nullable(String),
                        interaction_details_properties_code Nullable(String),
                        interaction_details_properties_deep_link Nullable(String),
                        interaction_details_properties_web_link Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''
                df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)

            elif(key == "CAMPAIGN_PLAY"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        campaign_details_selected_slot_index Nullable(Int64),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        interaction_details Nullable(String),
                        optional_payload_gratification_id Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''
                df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
                df['interaction_details'] = df['interaction_details'].astype(str, errors='ignore').fillna(0)

            elif(key == "ENTRY_POINT_CLICK"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        entry_point_data_entry_point_action_action_type Nullable(String),
                        entry_point_data_entry_point_action_open_container Nullable(String),
                        entry_point_data_entry_point_action_open_content_campaign_id Nullable(String),
                        entry_point_data_entry_point_action_open_content_static_url Nullable(String),
                        entry_point_data_entry_point_action_open_content_type Nullable(String),
                        entry_point_data_entry_point_container Nullable(String),
                        entry_point_data_entry_point_content_campaign_id Nullable(String),
                        entry_point_data_entry_point_content_static_url Nullable(String),
                        entry_point_data_entry_point_content_type Nullable(String),
                        entry_point_data_entry_point_id Nullable(String),
                        entry_point_data_entry_point_location Nullable(String),
                        entry_point_data_entry_point_name Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        timestamp DateTime,
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "CAMPAIGN_BANNER_CLICKED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        interaction_details Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
                df['interaction_details'] = df['interaction_details'].astype(str, errors='ignore').fillna(0)
            
            elif(key == "WEBVIEW_DISMISS"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        dismiss_trigger Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_absolute_height Nullable(String),
                        webview_content_campaignId Nullable(String),
                        webview_content_relative_height Nullable(String),
                        webview_content_webview_layout Nullable(String),
                        webview_content_webview_url Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "WEBVIEW_LOAD"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_absolute_height Nullable(String),
                        webview_content_campaignId Nullable(String),
                        webview_content_relative_height Nullable(String),
                        webview_content_webview_layout Nullable(String),
                        webview_content_webview_url Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "UI_INTERACTION"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        client Nullable(String),
                        eventId Nullable(String),
                        event_name Nullable(String),
                        event_properties Nullable(String),
                        headers Nullable(String),
                        timestamp DateTime NOT NULL,
                        userId Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''
                df['event_properties'] = df['event_properties'].astype(str, errors='ignore').fillna(0)

            elif(key == "QUIZ_QUESTION_ANSWERED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_details_campaign_experience Nullable(String),
                        campaign_details_campaign_id Nullable(String),
                        campaign_details_campaign_name Nullable(String),
                        campaign_details_campaign_state Nullable(String),
                        campaign_details_coupon_code Nullable(String),
                        campaign_details_reward_status Nullable(String),
                        campaign_details_reward_title Nullable(String),
                        campaign_details_reward_type Nullable(String),
                        campaign_details_reward_user_id Nullable(String),
                        campaign_details_selected_slot_index Nullable(Int64),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        interaction_details_quiz_details_correct_answer Nullable(String),
                        interaction_details_quiz_details_is_correct Nullable(UInt8),
                        interaction_details_quiz_details_question_index Nullable(UInt8),
                        interaction_details_quiz_details_session_id Nullable(String),
                        interaction_details_quiz_details_user_answer Nullable(String),
                        page_details_page_layout Nullable(String),
                        page_details_page_name Nullable(String),
                        platform_details_agent_type Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        referrer Nullable(String),
                        session_id Nullable(String),
                        session_time Nullable(Int64),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

                df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
                df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
                df['interaction_details_quiz_details_is_correct'] = df['interaction_details_quiz_details_is_correct'].astype(int, errors='ignore').fillna(0)
                df['interaction_details_quiz_details_question_index'] = df['interaction_details_quiz_details_question_index'].astype(int, errors='ignore').fillna(0)
            
            elif(key == "wallet_interaction"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        client Nullable(String),
                        eventId Nullable(String),
                        event_name Nullable(String),
                        event_properties_action Nullable(String),
                        event_properties_pageName Nullable(String),
                        headers Nullable(String),
                        timestamp DateTime NOT NULL,
                        userId Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''
            
            elif(key == "PIP_ENTRY_POINT_CTA_CLICK"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''
            
            elif(key == "MUTE_PIP_VIDEO"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "PIP_ENTRY_POINT_CLICK"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "PIP_ENTRY_POINT_LOAD"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "PIP_VIDEO_75_COMPLETED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "COLLAPSE_PIP_VIDEO"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "UNMUTE_PIP_VIDEO"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "PIP_VIDEO_COMPLETED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "PIP_VIDEO_25_COMPLETED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "PIP_VIDEO_50_COMPLETED"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "PIP_ENTRY_POINT_DISMISS"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            elif(key == "PIP_ENTRY_POINT_CLICK"):
                create_table_query = f'''
                    CREATE TABLE IF NOT EXISTS {key} (
                        analytics_version Nullable(String),
                        campaign_id Nullable(String),
                        client Nullable(String),
                        eventId Nullable(String),
                        event_id Nullable(String),
                        event_name Nullable(String),
                        headers Nullable(String),
                        platform_details_app_platform Nullable(String),
                        platform_details_device_type Nullable(String),
                        platform_details_os Nullable(String),
                        platform_details_sdk_version Nullable(String),
                        session_id Nullable(String),
                        timestamp DateTime NOT NULL,
                        type Nullable(String),
                        userId Nullable(String),
                        user_id Nullable(String),
                        webview_content_entry_point_container Nullable(String),
                        webview_content_entry_point_id Nullable(String),
                        webview_content_entry_point_is_expanded Nullable(String),
                        webview_content_entry_point_location Nullable(String),
                        webview_content_entry_point_name Nullable(String)
                    ) ENGINE = MergeTree()
                    ORDER BY (timestamp);
                '''

            else:
                continue

            # print(key)
            client.execute(create_table_query)

            client.insert_dataframe(f'INSERT INTO {key} VALUES', df,settings=dict(use_numpy=True))

        return True  

    except Exception as e:
        print(key)
        print("Error while storing data to ClickHouse: ", e)
        return False