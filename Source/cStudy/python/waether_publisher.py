import paho.mqtt.client as mqtt
import time
import json
import random
# import requests # 기상청 API 사용할 경우 주석 해제

# MQTT 설정
broker_address = "localhost" # Mosquitto 브로커 주소
port = 1883
topic = "digitaltwin/weather" # 언리얼에서 구독할 토픽

# MQTT 클라이언트 설정
client = mqtt.Client()

# 연결 시 호출되는 콜백 함수
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT Broker! Result Code: {rc}")
    else:
        print(f"Failed to connect, return code {rc}\n")

# 발행 확인 시 호출되는 콜백 함수
def on_publish(client, userdata, mid):
    print(f"Message published with MID {mid}")

client.on_connect = on_connect
client.on_publish = on_publish

# 브로커 연결 시도
client.connect(broker_address, port)

# 네트워크 루프 시작 (비동기 연결 및 콜백 처리를 위해 필요)
client.loop_start()

# 데이터 발행 함수
def publish_weather_data():
    # --- 데이터 생성 (임의 데이터 예시) ---
    temperature = round(random.uniform(5.0, 30.0), 2) # 5.0 ~ 30.0 사이 임의 온도
    humidity = round(random.uniform(30.0, 80.0), 2)    # 30.0 ~ 80.0 사이 임의 습도
    # weather_condition = random.choice(["Sunny", "Cloudy", "Rainy", "Snowy"]) # 날씨 상태 예시

    # --- 기상청 API 데이터 가져오기 예시 (API 키 필요) ---
    # try:
    #     api_key = "YOUR_API_KEY" # 실제 기상청 API 키로 변경
    #     # API 엔드포인트 및 파라미터 설정 (실제 API 문서 참고)
    #     url = f"http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst?serviceKey={api_key}&numOfRows=10&pageNo=1&base_date=YYYYMMDD&base_time=HHMM&nx=XX&ny=YY&dataType=JSON" # YYYYMMDD, HHMM, nx, ny 실제 값으로 변경
    #     response = requests.get(url)
    #     data = response.json()
    #     # API 응답에서 온도, 습도 등 필요한 정보 파싱
    #     # 예: items = data['response']['body']['items']['item']
    #     # temperature = ...
    #     # humidity = ...
    #     print("Weather data fetched from API.")
    # except Exception as e:
    #     print(f"Error fetching weather data from API: {e}")
    #     # API 호출 실패 시 임의 데이터 사용 또는 발행 중단
    #     temperature = -999 # 오류 표시 값
    #     humidity = -999
    #     # return # 발행 중단

    # 데이터를 JSON 형태로 구성
    payload = {
        "temperature": temperature,
        "humidity": humidity,
        # "condition": weather_condition, # 필요한 데이터 추가
        "timestamp": int(time.time()) # 타임스탬프 추가 (선택 사항)
    }

    # JSON 객체를 문자열로 변환
    payload_string = json.dumps(payload)

    # MQTT 메시지 발행
    # qos=0: 한 번만 보내기 (보장 없음), qos=1: 최소 한 번 보내기, qos=2: 정확히 한 번 보내기
    # retain=False: 브로커가 메시지를 저장하지 않음
    publish_result = client.publish(topic, payload_string, qos=1, retain=False)
    # publish_result.rc == mqtt.MQTT_ERR_SUCCESS 이면 발행 요청 성공

# 주기적으로 데이터 발행
try:
    while True:
        publish_weather_data()
        time.sleep(5) # 5초마다 데이터 발행
except KeyboardInterrupt:
    print("Publisher stopped by user.")
finally:
    client.loop_stop() # 네트워크 루프 중지
    client.disconnect() # 연결 끊기
    print("MQTT client disconnected.")