package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"git.ulinqdata.com/mauna/api/util/redis"
	"git.ulinqdata.com/vehicle/autolink2/util/orm"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/cast"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	//DEVICE_HEARTBEAT = "DEVICE_HEARTBEAT_"
	//Split            = "#"
	MQTT_PUSH_QUEUE  = "MQTT_PUSH_QUEUE"
	DEVICE_HEARTBEAT = "DEVICE_HEARTBEAT_"
)

const (
	MaxQueue = 100
)

var Payloads []Payload

type Payload struct {
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

var MessageQueue = make(chan Payload, 10000)

type Device struct {
	Id int
	Sn string
}

type DeviceLinkLog struct {
	Id int
	Sn string
}

type DeviceWarehouse struct {
	Id           int
	Sn           string
	Mac          string
	DeviceTypeId string
}

type DeviceType struct {
	Id         int
	Devicetype string
}

func main() {
	//initConfig()

	if err := orm.Init(&orm.Config{
		Driver:           "mysql",
		ConnectionString: "shimei:hYhdcNp4eMHd5SrY@tcp(127.0.0.1:3306)/shimei",
		TableNamePrefix:  "",
	}); err != nil {
		panic(fmt.Errorf("failed to init orm: %v", err))
	}
	InitMqtt()
	if err := redis.Init(&redis.Config{
		ConnectionString: "127.0.0.1:6379",
		Database:         0,
		Password:         "123456",
	}); err != nil {
		panic(fmt.Errorf("failed to init redis: %v", err))
	}

	//var devices []Device
	//sns := []string{""}
	//orm.DB().Table("device").Where("sn in (?)", "c45bbe68da6b").Find(&devices)
	////orm.DB().Table("device").Where("sn = ?", "c45bbe68da6b").Update("status", "-1")
	//
	//PrintString(device)

	go QueueHandler()

	Subscribe("/#", MqttHandler)

	// 优雅关闭，捕获程序中断（ctrl+c或docker stop等），并关闭所有链路
	sign := make(chan os.Signal, 1)
	signal.Notify(sign, os.Interrupt, os.Kill, syscall.SIGINT, syscall.SIGTERM)
	<-sign
}

func QueueHandler() {
	for payload := range MessageQueue {
		if strings.HasPrefix(payload.Message, "heart:") {
			Payloads = append(Payloads, payload)
			if len(Payloads) >= MaxQueue {
				var _payloads = make([]Payload, len(Payloads))
				copy(_payloads, Payloads)
				Payloads = []Payload{}
				go update(_payloads)
			}
		} else {
			bs, err := json.Marshal(payload)
			if err != nil {
				continue
			}
			redis.Session().LPush(MQTT_PUSH_QUEUE, string(bs))

			//go redisQueue(payload)
		}
	}
}

func update(payloads []Payload) {
	var macs []string
	for _, payload := range payloads {
		if payload.Topic == "" {
			continue
		}
		ss := strings.Split(payload.Topic, "/")
		if len(ss) < 3 {
			continue
		}
		macs = append(macs, ss[2])
		redis.Session().Set(DEVICE_HEARTBEAT+payload.Topic, true, time.Second*25)
	}
	if len(macs) == 0 {
		return
	}

	//var devices []Device
	//orm.DB().Table("device").Where("sn in (?)", sns).Find(&devices)

	//
	// Db::name("device")->where(["device_type" => type, "mac" => mac,"status"=>0])->update(["status"=>1]);
	// Db::name("device_warehouse")->where(["mac" => mac,"dstatus"=>0])->update(["dstatus"=>1]);

	orm.DB().Table("device").Where("mac in (?)", macs).Update("status", "1")
	orm.DB().Table("device_warehouse").Where("mac in (?)", macs).Update("dstatus", "1")
	fmt.Println("更新心跳成功")
}

func MqttHandler(client MQTT.Client, msg MQTT.Message) {
	MessageQueue <- Payload{
		Topic:   msg.Topic(),
		Message: string(msg.Payload()),
	}
}

//func initConfig() {
//	viper.SetConfigType("json")
//	viper.SetConfigFile("config.json")
//
//	err := viper.ReadInConfig()
//	if err != nil {
//		panic(fmt.Errorf("Fatal error config file: %s", err))
//	}
//}

// Init 初始化
// MQ MQTT 客户端
var MQ MQTT.Client

// do nothing
var f MQTT.MessageHandler = func(client MQTT.Client, msg MQTT.Message) {
}
var connectLostHandler MQTT.ConnectionLostHandler = func(client MQTT.Client, err error) {
	// 如果订阅后收不到消息 打开这个打印 看连接是不是被断开了 如果是很有可能是SetClientID的id重复了
	fmt.Printf("mqtt Connect lost: %v", err)
	//time.Sleep(time.Second*3)
	//InitMqtt()
}

func InitMqtt() {
	//if MQ.IsConnected() {
	//	println("mqtt已连接")
	//	return
	//}

	broker := "tcp://127.0.0.1:1883"
	opts := MQTT.NewClientOptions().AddBroker(broker)
	opts.SetClientID("shimei-server-master-1")
	opts.SetDefaultPublishHandler(f)
	opts.SetUsername("shimei888")
	opts.SetPassword("123456z.")
	opts.OnConnectionLost = connectLostHandler

	//logger = logger.With(
	//	zap.String("m2", "service/mqtt"),
	//)

	MQ = MQTT.NewClient(opts)
	if token := MQ.Connect(); token.Wait() && token.Error() != nil {
		panic("mqtt连接失败: " + token.Error().Error())
	}

	println("mqtt连接成功")

	//logger.Info("mqtt start",
	//	zap.String("broker", broker),
	//	zap.String("namespace", namespace),
	//)
}

// Subscribe 订阅消息
func Subscribe(topic string, callback MQTT.MessageHandler) {
	if MQ != nil {
		token := MQ.Subscribe(topic, 0, callback)
		if token.Wait() && token.Error() != nil {
			panic("订阅失败: " + token.Error().Error())
		}
	}
}

func PrintString(data interface{}) {
	b, err := json.Marshal(data)
	if err != nil {
		fmt.Println("%+v", data)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "    ")
	if err != nil {
		fmt.Println("%+v", data)
	}

	println(out.String())
}

// ================================================================================================================>
func redisQueue(payload Payload) {
	PrintString(payload)

	if payload.Topic == "" {
		return
	}
	ss := strings.Split(payload.Topic, "/")
	if len(ss) < 3 {
		return
	}
	_type := ss[1]
	_mac := ss[2]

	ss1 := strings.Split(payload.Message, ":")
	if len(ss1) < 2 {
		return
	}
	_button := ss1[0]
	_key := ss1[1]

	fmt.Println(fmt.Sprintf("type: %s, mac: %s, button: %s, key: %s", _type, _mac, _button, _key))

	data := make(map[string]interface{})
	switch _button {
	case "power": //开关机
		data["kaiguan"] = cast.ToInt(_key)
	case "mode": //模式
		data["moshi"] = cast.ToInt(_key)
	case "tempr": //温度
		data["wendu"] = cast.ToInt(_key)
	case "shum": //设置湿度
		data["set_shidu"] = cast.ToInt(_key)
	case "hum": //湿度
		data["shidu"] = cast.ToInt(_key)
	case "speed": //风速
		data["fengsu"] = cast.ToInt(_key)
	case "ion": //负离子
		data["fulizi"] = cast.ToInt(_key)
	case "lock": //童锁
		data["tongsuo"] = cast.ToInt(_key)
	case "defrost": //化霜温度
		data["huashuangwendu"] = cast.ToInt(_key)
	case "flag": //状态
		if _key == "ba" {
			data["errflag"] = ""
			data["status"] = 1
		} else {
			data["errflag"] = _key
			data["status"] = -1
		}
	case "old_token":
		token := _key
		var link DeviceLinkLog
		orm.DB().Table("device_link_log").Where("up_token = ?", token).First(&link)
		if link.Sn != "" {
			//配网日志
			updateValues := map[string]interface{}{
				"mac":    _mac,
				"status": "1",
			}
			orm.DB().Table("device_link_log").Where("id = ?", link.Id).Update(updateValues)

			//
			var dw DeviceWarehouse
			orm.DB().Table("device_link_log").Where("sn = ?", link.Sn).First(&dw)
			if dw.Sn == "" {
				//设备库
				updateValues = map[string]interface{}{
					"mac":    _mac,
					"status": "1",
				}
				orm.DB().Table("device_warehouse").Where("id = ?", dw.Id).Update(updateValues)
			}
			//用户设备
			updateValues = map[string]interface{}{
				"mac":    _mac,
				"status": "1",
			}
			orm.DB().Table("device").Where("sn = ? and mac = ?", link.Sn, "").Update(updateValues)
		}
	case "token":
		token := _key
		var link DeviceLinkLog
		orm.DB().Table("device_link_log").Where("up_token = ?", token).First(&link)
		if link.Sn != "" {
			//配网日志
			updateValues := map[string]interface{}{
				"mac":    _mac,
				"status": "1",
				"type":   _type,
				"sn":     _mac,
			}
			orm.DB().Table("device_link_log").Where("id = ? ", link.Id).Update(updateValues)
			link.Sn = _mac
			//型号
			var dt DeviceType
			orm.DB().Table("device_type").Where("devicetype = ?", _type).First(&dt)
			//基础设备
			var dw DeviceWarehouse
			orm.DB().Table("device_warehouse").Where("sn = ?", _mac).First(&dw)
			if dw.Sn == "" {
				save := map[string]interface{}{
					"sn":             _mac,
					"mac":            _mac,
					"device_type_id": cast.ToString(dt.Id),
					"createtime":     time.Now().Unix(),
				}
				orm.DB().Table("device_warehouse").Create(&save)
				orm.DB().Table("device_warehouse").Where("sn = ?", _mac).First(&dw)
			}
			if dw.Mac == "" {
				//设备库
				updateValues = map[string]interface{}{
					"mac":    _mac,
					"status": "1",
				}
				orm.DB().Table("device_warehouse").Where("id = ?", dw.Id).Update(updateValues)
			}
			//用户设备
			updateValues = map[string]interface{}{
				"mac":    _mac,
				"status": "1",
			}
			orm.DB().Table("device").Where("sn = ? and mac = ?", link.Sn, "").Update(updateValues)
		}
	}
	if len(data) > 0 {
		if val, ok := data["status"]; ok {
			data["status"] = cast.ToString(val)
		}
		if val, ok := data["kaiguan"]; ok {
			data["kaiguan"] = cast.ToString(val)
		}
		if val, ok := data["tongsuo"]; ok {
			data["tongsuo"] = cast.ToString(val)
		}
		if val, ok := data["fulizi"]; ok {
			data["fulizi"] = cast.ToString(val)
		}
		if val, ok := data["fengsu"]; ok {
			data["fengsu"] = cast.ToString(val)
		}
		if val, ok := data["moshi"]; ok {
			data["moshi"] = cast.ToString(val)
		}
		if val, ok := data["huashuangwendu"]; ok {
			data["huashuangwendu"] = cast.ToString(val)
		}

		var device Device
		orm.DB().Table("device").Where("device_type = ? and mac = ?", _type, _mac).First(&device)
		orm.DB().Table("device").Where("device_type = ? and mac = ?", _type, _mac).Update(data)
		if _, ok := data["status"]; ok {
			data["dstatus"] = data["status"]
			delete(data, "status")
		}
		orm.DB().Table("device_warehouse").Where("mac = ?", _mac).Update(data)
	}
}
