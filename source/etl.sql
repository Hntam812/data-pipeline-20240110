CREATE PROCEDURE usp_ETL_Process
AS
BEGIN
    -- Bước 0: Logging - Bắt đầu quá trình ETL
    INSERT INTO ETL_Log (Action, LogTime) VALUES ('Start ETL Process', GETDATE());

    -- Chọn cơ sở dữ liệu SQL Staging
    USE CampaignDB;

    BEGIN TRY
        -- Bước 1: Logging - Đọc tệp từ local và Import vào bảng StagingCampaign
        INSERT INTO ETL_Log (Action, LogTime) VALUES ('Start Reading and Importing', GETDATE());

        TRUNCATE TABLE StagingCampaign; -- Xóa dữ liệu cũ trong bảng tạm

        BULK INSERT StagingCampaign
        FROM '/home/demi/demi/data-pipeline/data/incoming/data.csv'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            FIRSTROW = 2,
            DATAFILETYPE = 'char',
            TABLOCK
        );

        -- Bước 2: Thực hiện quá trình ETL từ bảng StagingCampaign vào bảng Campaign và Campaign_delta
        MERGE INTO Campaign AS target
        USING StagingCampaign AS source
        ON target.CAMPAIGN_CODE = source.CAMPAIGN_CODE
        WHEN MATCHED THEN
            UPDATE SET
                target.CAMPAIGN_NAME = source.CAMPAIGN_NAME,
                target.CAMPAIGN_START_DATE = source.CAMPAIGN_START_DATE,
                target.CAMPAIGN_END_DATE = source.CAMPAIGN_END_DATE,
                target.PRE_CAMPAIGN_START_DATE = source.PRE_CAMPAIGN_START_DATE,
                target.PRE_CAMPAIGN_END_DATE = source.PRE_CAMPAIGN_END_DATE,
                target.CAMPAIGN_BUDGET = source.CAMPAIGN_BUDGET,
                target.CAMPAIGN_EXPENSE = source.CAMPAIGN_EXPENSE,
                target.CAMPAIGN_DESC = source.CAMPAIGN_DESC,
                target.CAMPAIGN_INITIATOR = source.CAMPAIGN_INITIATOR,
                target.CAMPAIGN_MANAGER = source.CAMPAIGN_MANAGER,
                target.REVENUE_PLAN = source.REVENUE_PLAN,
                target.TARGET_CUST_COUNT = source.TARGET_CUST_COUNT,
                target.ACTIVE = source.ACTIVE
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                CAMPAIGN_CODE,
                CAMPAIGN_NAME,
                CAMPAIGN_START_DATE,
                CAMPAIGN_END_DATE,
                PRE_CAMPAIGN_START_DATE,
                PRE_CAMPAIGN_END_DATE,
                CAMPAIGN_BUDGET,
                CAMPAIGN_EXPENSE,
                CAMPAIGN_DESC,
                CAMPAIGN_INITIATOR,
                CAMPAIGN_MANAGER,
                REVENUE_PLAN,
                TARGET_CUST_COUNT,
                ACTIVE
            )
            VALUES (
                source.CAMPAIGN_CODE,
                source.CAMPAIGN_NAME,
                source.CAMPAIGN_START_DATE,
                source.CAMPAIGN_END_DATE,
                source.PRE_CAMPAIGN_START_DATE,
                source.PRE_CAMPAIGN_END_DATE,
                source.CAMPAIGN_BUDGET,
                source.CAMPAIGN_EXPENSE,
                source.CAMPAIGN_DESC,
                source.CAMPAIGN_INITIATOR,
                source.CAMPAIGN_MANAGER,
                source.REVENUE_PLAN,
                source.TARGET_CUST_COUNT,
                source.ACTIVE
            );

        -- Bước 3: Logging - Watch CDC from Campaign to Campaign_delta
        INSERT INTO ETL_Log (Action, LogTime) VALUES ('Start CDC Process', GETDATE());

        -- Bước 4: Watch CDC from Campaign to Campaign_delta
        DECLARE @from_lsn BINARY(10), @to_lsn BINARY(10);
        SELECT @from_lsn = MIN(__$start_lsn), @to_lsn = MAX(__$start_lsn)
        FROM cdc.fn_cdc_get_all_changes_dbo_Campaign(NULL, NULL, 'all');

        INSERT INTO Campaign_delta
        SELECT
            cd.CAMPAIGN_CODE,
            cd.CAMPAIGN_NAME,
            cd.CAMPAIGN_START_DATE,
            cd.CAMPAIGN_END_DATE,
            cd.PRE_CAMPAIGN_START_DATE,
            cd.PRE_CAMPAIGN_END_DATE,
            cd.CAMPAIGN_BUDGET,
            cd.CAMPAIGN_EXPENSE,
            cd.CAMPAIGN_DESC,
            cd.CAMPAIGN_INITIATOR,
            cd.CAMPAIGN_MANAGER,
            cd.REVENUE_PLAN,
            cd.TARGET_CUST_COUNT,
            cd.ACTIVE,
            CASE
                WHEN ct.__$operation = 1 THEN 'I' -- Insert
                WHEN ct.__$operation = 2 THEN 'U' -- Update
                WHEN ct.__$operation = 3 THEN 'D' -- Delete
                ELSE NULL
            END AS CHANGE_TYPE,
            ct.__$timestamp AS CHANGE_DATE
        FROM
            Campaign_delta cd
        INNER JOIN
            cdc.fn_cdc_get_net_changes_dbo_Campaign(@from_lsn, @to_lsn, 'all') ct
        ON
            cd.__$start_lsn = ct.__$start_lsn
            AND cd.__$seqval = ct.__$seqval;

        -- Bước 5: Logging - Export data Campaign_delta to CSV to local
        INSERT INTO ETL_Log (Action, LogTime) VALUES ('Start Exporting to CSV', GETDATE());

        DECLARE @output_file_path NVARCHAR(255) = '/home/demi/demi/data-pipeline/data/output/Campaign_changes.csv';
        EXEC xp_cmdshell 'sqlcmd -S server_name -d CampaignDB -U username -P password -Q "SELECT * FROM Campaign_delta" -o ' + @output_file_path + ' -h-1 -s","';

        -- Bước 6: Logging - Send Email
        INSERT INTO ETL_Log (Action, LogTime) VALUES ('Start Sending Email', GETDATE());

        DECLARE @subject NVARCHAR(255);
        DECLARE @body NVARCHAR(MAX);
        DECLARE @attachment NVARCHAR(255);

        -- Tạo nội dung email
        SET @subject = 'ETL Process Notification';
        SET @body = 'Quá trình ETL đã hoàn thành vào lúc ' + CONVERT(NVARCHAR, GETDATE(), 120);

        -- Đường dẫn đến tệp CSV cần đính kèm
        SET @attachment = '/home/demi/demi/data-pipeline/data/output/Campaign_changes.csv';

        -- Gửi email
        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = 'YourMailProfile', -- Tên cấu hình mail đã được cấu hình trước
            @recipients = 'hongoctam0812@gmail.com', -- Địa chỉ email của người nhận
            @subject = @subject,
            @body = @
            @file_attachments = @attachment;

        -- Bước 7: Logging - Kết thúc quá trình ETL
        INSERT INTO ETL_Log (Action, LogTime) VALUES ('End ETL Process', GETDATE());
    END TRY

    BEGIN CATCH
        -- Bắt lỗi và ghi log
        INSERT INTO ETL_Log (Action, LogTime, Status, ErrorMessage)
        VALUES ('Error during ETL Process', GETDATE(), 'Failure', ERROR_MESSAGE());
    END CATCH;
    
END;