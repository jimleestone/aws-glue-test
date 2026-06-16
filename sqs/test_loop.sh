#!/bin/bash

# Set environment variable
ENV=dev

# source queue name
QUEUE_NAME="$1"

# source max execute count and interval(seconds)
MAX_EXECUTE_COUNT="$2"
INTERVAL="$3"

# max waiting time(seconds) within a SQS polling request
MAX_WAIT="$4"

# Shift the positional parameters by 2
# This will remove the first three parameters (QUEUE_NAME, MAX_EXECUTE_COUNT, INTERVAL) and leave only the job names in the positional parameters
shift 4

# Capture all target job name arguments into a array
JOB_LIST=("$@")

# SQS base parameters
REGION="ap-northeast-1"
QUEUE_URL="https://sqs.$REGION.amazonaws.com/379867926836/$QUEUE_NAME"

ERR_CD=0
for ((i = 1; i <= $MAX_EXECUTE_COUNT; i++)); do
	echo "Execution Count: $i"
	# メッセージをロングポーリングで取得（待機時間20秒）
	RESPONSE=$(aws sqs receive-message \
		--queue-url "$QUEUE_URL" \
		--region "$REGION" \
		--wait-time-seconds "$MAX_WAIT" \
		--max-number-of-messages 1)

	# メッセージが存在するか判定
	MESSAGE_BODY=$(echo "$RESPONSE" | jq -r '.Messages[0].Body')

	if [ "$MESSAGE_BODY" != "null" ] && [ -n "$MESSAGE_BODY" ]; then
		# Check if the jobName in the message body matches any of the job names in the JOB_LIST array
		if jq -e --argjson valid_vals "$(printf '%s\n' "${JOB_LIST[@]}" | jq -R . | jq -s .)" \
			'.detail.jobName | select(. != null) as $val | $valid_vals | index($val)' <<<"$MESSAGE_BODY" >/dev/null; then
			echo "メッセージを受信しました: $MESSAGE_BODY"

			# Check the job state and set the appropriate error code
			case "$(echo "$MESSAGE_BODY" | jq '.detail.state' -r)" in
			SUCCEEDED)
				echo "ジョブが成功しました。"
				ERR_CD=200
				;;
			FAILED)
				echo "ジョブが失敗しました。"
				ERR_CD=500
				;;
			TIMEOUT)
				echo "ジョブがタイムアウトしました。"
				ERR_CD=504
				;;
			*)
				echo "認識できないジョブステータスです。"
				;;
			esac

			# Print jobName, jobRunId, and message from the message body
			echo "jobName: $(echo $MESSAGE_BODY | jq -r '.detail.jobName'), jobRunId: $(echo $MESSAGE_BODY | jq -r '.detail.jobRunId'), message: $(echo $MESSAGE_BODY | jq '.detail.message' -r)"

			# 処理が完了したらメッセージを削除（ReceiptHandleを使用）
			RECEIPT_HANDLE=$(echo "$RESPONSE" | jq -r '.Messages[0].ReceiptHandle')
			aws sqs delete-message \
				--queue-url "$QUEUE_URL" \
				--region "$REGION" \
				--receipt-handle "$RECEIPT_HANDLE"

			echo "処理が完了され、メッセージを削除しました。"
			break
		else
			echo "メッセージは有効ではありません: $MESSAGE_BODY"
			echo "次のメッセージを待機します..."
			sleep $MAX_WAIT
			continue
		fi
	else
		echo "メッセージが存在しません。次の実行まで待機します..."
		sleep $INTERVAL
	fi
done

echo "処理結果コード: $ERR_CD"
