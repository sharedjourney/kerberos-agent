package cloud

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dromara/carbon/v2"
	"github.com/elastic/go-sysinfo"
	"github.com/gin-gonic/gin"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"net/http"
	"strconv"
	"time"

	"github.com/kerberos-io/agent/machinery/src/capture"
	"github.com/kerberos-io/agent/machinery/src/cloud/livesnapshot"
	"github.com/kerberos-io/agent/machinery/src/encryption"
	"github.com/kerberos-io/agent/machinery/src/log"
	"github.com/kerberos-io/agent/machinery/src/models"
	"github.com/kerberos-io/agent/machinery/src/onvif"
	"github.com/kerberos-io/agent/machinery/src/packets"
	"github.com/kerberos-io/agent/machinery/src/utils"
	"github.com/kerberos-io/agent/machinery/src/webrtc"
)

func PendingUpload(configDirectory string) {
	ff, err := utils.ReadDirectory(configDirectory + "/data/cloud/")
	if err == nil {
		for _, f := range ff {
			log.Log.Info(f.Name())
		}
	}
}

func HandleUpload(configDirectory string, configuration *models.Configuration, communication *models.Communication) {

	log.Log.Debug("HandleUpload: started")

	config := configuration.Config
	watchDirectory := configDirectory + "/data/cloud/"

	if config.Offline == "true" {
		log.Log.Debug("HandleUpload: stopping as Offline is enabled.")
	} else {

		// Half a second delay between two uploads
		delay := 500 * time.Millisecond

	loop:
		for {
			// This will check if we need to stop the thread,
			// because of a reconfiguration.
			select {
			case <-communication.HandleUpload:
				break loop
			case <-time.After(2 * time.Second):
			}

			ff, err := utils.ReadDirectory(watchDirectory)
			if err != nil {
				log.Log.Error("HandleUpload: " + err.Error())
			} else {
				for _, f := range ff {

					// This will check if we need to stop the thread,
					// because of a reconfiguration.
					select {
					case <-communication.HandleUpload:
						break loop
					default:
					}

					fileName := f.Name()
					uploaded := false
					configured := false
					err = nil
					if config.Cloud == "s3" || config.Cloud == "kerberoshub" {
						uploaded, configured, err = UploadKerberosHub(configuration, fileName)
					} else if config.Cloud == "kstorage" || config.Cloud == "kerberosvault" {
						uploaded, configured, err = UploadKerberosVault(configuration, fileName)
					} else if config.Cloud == "dropbox" {
						uploaded, configured, err = UploadDropbox(configuration, fileName)
					} else if config.Cloud == "gdrive" {
						// Todo: implement gdrive upload
					} else if config.Cloud == "onedrive" {
						// Todo: implement onedrive upload
					} else if config.Cloud == "minio" {
						// Todo: implement minio upload
					} else if config.Cloud == "webdav" {
						// Todo: implement webdav upload
					} else if config.Cloud == "ftp" {
						// Todo: implement ftp upload
					} else if config.Cloud == "sftp" {
						// Todo: implement sftp upload
					} else if config.Cloud == "aws" {
						// Todo: need to be updated, was previously used for hub.
						uploaded, configured, err = UploadS3(configuration, fileName)
					} else if config.Cloud == "azure" {
						// Todo: implement azure upload
					} else if config.Cloud == "google" {
						// Todo: implement google upload
					}
					// And so on... (have a look here -> https://github.com/kerberos-io/agent/issues/95)

					// Check if the file is uploaded, if so, remove it.
					if uploaded {
						delay = 500 * time.Millisecond // reset
						err := os.Remove(watchDirectory + fileName)
						if err != nil {
							log.Log.Error("HandleUpload: " + err.Error())
						}

						// Check if we need to remove the original recording
						// removeAfterUpload is set to false by default
						if config.RemoveAfterUpload != "false" {
							err := os.Remove(configDirectory + "/data/recordings/" + fileName)
							if err != nil {
								log.Log.Error("HandleUpload: " + err.Error())
							}
						}
					} else if !configured {
						err := os.Remove(watchDirectory + fileName)
						if err != nil {
							log.Log.Error("HandleUpload: " + err.Error())
						}
					} else {
						delay = 5 * time.Second // slow down
						if err != nil {
							log.Log.Error("HandleUpload: " + err.Error())
						}
					}

					time.Sleep(delay)
				}
			}
		}
	}

	log.Log.Debug("HandleUpload: finished")
}

func GetSystemInfo() (models.System, error) {
	var usedMem uint64 = 0
	var totalMem uint64 = 0
	var freeMem uint64 = 0

	var processUsedMem uint64 = 0

	architecture := ""
	cpuId := ""
	KernelVersion := ""
	agentVersion := ""
	var MACs []string
	var IPs []string
	hostname := ""
	bootTime := time.Time{}

	// Read agent version
	version, err := os.Open("./version")
	agentVersion = "unknown"
	if err == nil {
		defer version.Close()
		agentVersionBytes, err := io.ReadAll(version)
		agentVersion = string(agentVersionBytes)
		if err != nil {
			log.Log.Error(err.Error())
		}
	}

	host, err := sysinfo.Host()
	if err == nil {
		cpuId = host.Info().UniqueID
		architecture = host.Info().Architecture
		KernelVersion = host.Info().KernelVersion
		MACs = host.Info().MACs
		IPs = host.Info().IPs
		hostname = host.Info().Hostname
		bootTime = host.Info().BootTime
		memory, err := host.Memory()
		if err == nil {
			usedMem = memory.Used
			totalMem = memory.Total
			freeMem = memory.Free
		}
	}

	process, err := sysinfo.Self()
	if err == nil {
		memInfo, err := process.Memory()
		if err == nil {
			processUsedMem = memInfo.Resident
		}
	}

	system := models.System{
		Hostname:          hostname,
		CPUId:             cpuId,
		KernelVersion:     KernelVersion,
		Version:           agentVersion,
		MACs:              MACs,
		IPs:               IPs,
		BootTime:          uint64(bootTime.Unix()),
		Architecture:      architecture,
		UsedMemory:        usedMem,
		TotalMemory:       totalMem,
		FreeMemory:        freeMem,
		ProcessUsedMemory: processUsedMem,
	}

	return system, nil
}

const (
	// heartbeatHTTPTimeout bounds the POST to Hub/Vault so a stalled
	// connection delays one beat instead of wedging the loop.
	heartbeatHTTPTimeout = 30 * time.Second
	// onvifMetadataInterval is how often the background poller refreshes
	// ONVIF capabilities. They change rarely, so this is deliberately
	// slow — it only needs to stay ahead of operator-visible drift, not
	// the 10s heartbeat cadence.
	onvifMetadataInterval = 60 * time.Second
)

// pollONVIFMetadata refreshes the shared ONVIF metadata cache on its own
// cadence so HandleHeartBeat never performs blocking camera I/O on the
// send path. It primes the cache once immediately, then ticks until
// stopped. The blocking work is bounded by the ONVIF client timeout.
func pollONVIFMetadata(configuration *models.Configuration, stop <-chan struct{}, cache *onvif.MetadataCache) {
	refresh := func() {
		// Read only the ONVIF connection fields rather than copying the
		// whole IPCamera value: RunAgent writes other IPCamera fields
		// (Width/Height/BaseHeight) on each (re)start from a different
		// goroutine, so a whole-struct copy here would be an
		// unsynchronised read. These three are never written after config
		// load. Re-reading the live config each tick is what lets a
		// removed camera fall back to defaults on the next poll.
		src := configuration.Config.Capture.IPCamera
		camera := models.IPCamera{
			ONVIFXAddr:    src.ONVIFXAddr,
			ONVIFUsername: src.ONVIFUsername,
			ONVIFPassword: src.ONVIFPassword,
		}
		cache.Store(onvif.GatherHeartbeatMetadata(&camera))
	}
	refresh()

	ticker := time.NewTicker(onvifMetadataInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func HandleHeartBeat(configuration *models.Configuration, communication *models.Communication, uptimeStart time.Time) {
	log.Log.Debug("cloud.HandleHeartBeat(): started")

	var client *http.Client
	if os.Getenv("AGENT_TLS_INSECURE") == "true" {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client = &http.Client{Transport: tr, Timeout: heartbeatHTTPTimeout}
	} else {
		client = &http.Client{Timeout: heartbeatHTTPTimeout}
	}

	// Refresh ONVIF metadata off the heartbeat send path so a wedged
	// camera can never stall the heartbeat. The cache is local: this
	// function is its only writer (via the poller) and reader (below).
	// Stops when this function returns (reconfiguration / shutdown).
	// Two caches with deliberately different scopes: metadataCache has a
	// single owner (the poller below writes it, this loop reads it, both
	// live here) so it is a local; eventCache is shared across goroutines
	// (event-stream writer, this reader, the HTTP I/O endpoints) so it is
	// created once at bootstrap and carried on Communication. Do not move
	// metadataCache onto Communication — the locality is intentional.
	metadataCache := onvif.NewMetadataCache()
	eventCache := onvif.EventCacheFor(communication)
	stopMetadata := make(chan struct{})
	defer close(stopMetadata)
	go pollONVIFMetadata(configuration, stopMetadata, metadataCache)

	kerberosAgentVersion := utils.VERSION

loop:
	for {
		// Configuration migh have changed, so we will reload it.
		config := configuration.Config

		// ONVIF capabilities (PTZ/presets/digital-I/O) are refreshed by a
		// background poller into a shared cache; the heartbeat reads that
		// snapshot instead of talking to the camera, so a slow ONVIF call
		// can never delay a heartbeat. Live digital-I/O state still comes
		// from the event-stream cache, with the poller's enumerated token
		// list as a non-blocking fallback.
		onvifMetadata := metadataCache.Snapshot()
		onvifEnabled := onvifMetadata.Enabled
		onvifZoom := onvifMetadata.Zoom
		onvifPanTilt := onvifMetadata.PanTilt
		onvifPresets := onvifMetadata.Presets
		onvifPresetsList := onvifMetadata.PresetsList
		onvifEventsList := onvif.AssembleHeartbeatEvents(eventCache, onvifMetadata.IOFallback)

		// We'll capture some more metrics, and send it to Hub, if not in offline mode ofcourse ;) ;)
		if config.Offline == "true" {
			log.Log.Debug("cloud.HandleHeartBeat(): stopping as Offline is enabled.")
		} else {

			hubURI := config.HeartbeatURI
			key := ""
			username := ""
			vaultURI := ""

			if config.Cloud == "s3" && config.S3 != nil && config.S3.Publickey != "" {
				username = config.S3.Username
				key = config.S3.Publickey
			} else if config.Cloud == "kstorage" && config.KStorage != nil && config.KStorage.CloudKey != "" {
				key = config.KStorage.CloudKey
				username = config.KStorage.Directory
			}

			// This is the new way ;)
			if config.HubURI != "" {
				hubURI = config.HubURI + "/devices/heartbeat"
			}
			if config.HubKey != "" {
				key = config.HubKey
			}

			// Check if we have a friendly name or not.
			name := config.Name
			if config.FriendlyName != "" {
				name = config.FriendlyName
			}

			// Get some system information
			// like the uptime, hostname, memory usage, etc.
			system, _ := GetSystemInfo()

			// Check if the agent is running inside a cluster (Kerberos Factory) or as
			// an open source agent
			isEnterprise := false
			if os.Getenv("DEPLOYMENT") == "factory" || os.Getenv("MACHINERY_ENVIRONMENT") == "kubernetes" {
				isEnterprise = true
			}

			// Congert to string
			macs, _ := json.Marshal(system.MACs)
			ips, _ := json.Marshal(system.IPs)
			cameraConnected := "true"
			if !communication.CameraConnected {
				cameraConnected = "false"
			}

			hasBackChannel := "false"
			if communication.HasBackChannel {
				hasBackChannel = "true"
			}

			hub_encryption := "false"
			if config.HubEncryption == "true" {
				hub_encryption = "true"
			}

			e2e_encryption := "false"
			if config.Encryption != nil && config.Encryption.Enabled == "true" {
				e2e_encryption = "true"
			}

			// We will formated the uptime to a human readable format
			// this will be used on Kerberos Hub: Uptime -> 1 day and 2 hours.
			uptimeFormatted := uptimeStart.Format("2006-01-02 15:04:05")
			uptimeString := carbon.Parse(uptimeFormatted).DiffForHumans()
			uptimeString = strings.ReplaceAll(uptimeString, "ago", "")

			// Do the same for boottime
			bootTimeFormatted := time.Unix(int64(system.BootTime), 0).Format("2006-01-02 15:04:05")
			boottimeString := carbon.Parse(bootTimeFormatted).DiffForHumans()
			boottimeString = strings.ReplaceAll(boottimeString, "ago", "")

			// We need a hub URI and hub public key before we will send a heartbeat
			if hubURI != "" && key != "" {

				var object = fmt.Sprintf(`{
						"key" : "%s",
						"version" : "%s",
						"hub_encryption": "%s",
						"e2e_encryption": "%s",
						"release" : "%s",
						"cpuid" : "%s",
						"clouduser" : "%s",
						"cloudpublickey" : "%s",
						"cameraname" : "%s",
						"enterprise" : %t,
						"hostname" : "%s",
						"architecture" : "%s",
						"totalMemory" : "%d",
						"usedMemory" : "%d",
						"freeMemory" : "%d",
						"processMemory" : "%d",
						"mac_list" : %s,
						"ip_list" : %s,
						"board" : "",
						"disk1size" : "%s",
						"disk3size" : "%s",
						"diskvdasize" :  "%s",
						"uptime" : "%s",
						"boot_time" : "%s",
						"siteID" : "%s",
						"onvif" : "%s",
						"onvif_zoom" : "%s",
						"onvif_pantilt" : "%s",
						"onvif_presets": "%s",
						"onvif_presets_list": %s,
						"onvif_events_list": %s,
						"cameraConnected": "%s",
						"hasBackChannel": "%s",
						"livePreviewHttp": true,
						"numberoffiles" : "33",
						"timestamp" : 1564747908,
						"cameratype" : "IPCamera",
						"docker" : true,
						"kios" : false,
						"raspberrypi" : false
					}`, config.Key, kerberosAgentVersion, hub_encryption, e2e_encryption, system.Version, system.CPUId, username, key, name, isEnterprise, system.Hostname, system.Architecture, system.TotalMemory, system.UsedMemory, system.FreeMemory, system.ProcessUsedMemory, macs, ips, "0", "0", "0", uptimeString, boottimeString, config.HubSite, onvifEnabled, onvifZoom, onvifPanTilt, onvifPresets, onvifPresetsList, onvifEventsList, cameraConnected, hasBackChannel)

				// Get the private key to encrypt the data using symmetric encryption: AES.
				privateKey := config.HubPrivateKey
				if hub_encryption == "true" && privateKey != "" {
					// Encrypt the data using AES.
					encrypted, err := encryption.AesEncrypt([]byte(object), privateKey)
					if err != nil {
						encrypted = []byte("")
						log.Log.Error("cloud.HandleHeartBeat(): error while encrypting data: " + err.Error())
					}

					// Base64 encode the encrypted data.
					encryptedBase64 := base64.StdEncoding.EncodeToString(encrypted)
					object = fmt.Sprintf(`{
						"cloudpublicKey": "%s",
						"encrypted" : %t,
						"encryptedData" : "%s"
					}`, config.HubKey, true, encryptedBase64)
				}

				var jsonStr = []byte(object)
				buffy := bytes.NewBuffer(jsonStr)
				req, _ := http.NewRequest("POST", hubURI, buffy)
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if resp != nil {
					resp.Body.Close()
				}
				if err == nil && resp.StatusCode == 200 {
					communication.CloudTimestamp.Store(time.Now().Unix())
					log.Log.Info("cloud.HandleHeartBeat(): (200) Heartbeat received by Kerberos Hub.")
				} else {
					if communication.CloudTimestamp != nil && communication.CloudTimestamp.Load() != nil {
						communication.CloudTimestamp.Store(int64(0))
					}
					log.Log.Error("cloud.HandleHeartBeat(): (400) Something went wrong while sending to Kerberos Hub.")
				}
			} else {
				log.Log.Error("cloud.HandleHeartBeat(): Disabled as we do not have a public key defined.")
			}

			// If we have a Kerberos Vault connected, we will also send some analytics
			// to that service.
			vaultURI = config.KStorage.URI
			accessKey := config.KStorage.AccessKey
			secretAccessKey := config.KStorage.SecretAccessKey
			if vaultURI != "" && accessKey != "" && secretAccessKey != "" {

				var object = fmt.Sprintf(`{
					"key" : "%s",
					"version" : "%s",
					"release" : "%s",
					"cpuid" : "%s",
					"clouduser" : "%s",
					"cloudpublickey" : "%s",
					"cameraname" : "%s",
					"enterprise" : %t,
					"hostname" : "%s",
					"architecture" : "%s",
					"totalMemory" : "%d",
					"usedMemory" : "%d",
					"freeMemory" : "%d",
					"processMemory" : "%d",
					"mac_list" : %s,
					"ip_list" : %s,
					"board" : "",
					"disk1size" : "%s",
					"disk3size" : "%s",
					"diskvdasize" :  "%s",
					"uptime" : "%s",
					"boot_time" : "%s",
					"siteID" : "%s",
					"onvif" : "%s",
					"onvif_zoom" : "%s",
					"onvif_pantilt" : "%s",
					"onvif_presets": "%s",
					"onvif_presets_list": %s,
					"cameraConnected": "%s",
					"numberoffiles" : "33",
					"timestamp" : 1564747908,
					"cameratype" : "IPCamera",
					"docker" : true,
					"kios" : false,
					"raspberrypi" : false
				}`, config.Key, kerberosAgentVersion, system.Version, system.CPUId, username, key, name, isEnterprise, system.Hostname, system.Architecture, system.TotalMemory, system.UsedMemory, system.FreeMemory, system.ProcessUsedMemory, macs, ips, "0", "0", "0", uptimeString, boottimeString, config.HubSite, onvifEnabled, onvifZoom, onvifPanTilt, onvifPresets, onvifPresetsList, cameraConnected)

				var jsonStr = []byte(object)
				buffy := bytes.NewBuffer(jsonStr)
				req, _ := http.NewRequest("POST", vaultURI+"/devices/heartbeat", buffy)
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if resp != nil {
					resp.Body.Close()
				}
				if err == nil && resp.StatusCode == 200 {
					log.Log.Info("cloud.HandleHeartBeat(): (200) Heartbeat received by Kerberos Vault.")
				} else {
					log.Log.Error("cloud.HandleHeartBeat(): (400) Something went wrong while sending to Kerberos Vault.")
				}
			}
		}

		// This will check if we need to stop the thread,
		// because of a reconfiguration.
		select {
		case <-communication.HandleHeartBeat:
			break loop
		case <-time.After(10 * time.Second):
		}
	}

	log.Log.Debug("cloud.HandleHeartBeat(): finished")
}

func HandleLiveStreamSD(livestreamCursor *packets.QueueCursor, configuration *models.Configuration, communication *models.Communication, mqttClient mqtt.Client, rtspClient capture.RTSPClient) {

	log.Log.Debug("cloud.HandleLiveStreamSD(): started")

	config := configuration.Config

	// If offline made is enabled, we will stop the thread.
	if config.Offline == "true" {
		log.Log.Debug("cloud.HandleLiveStreamSD(): stopping as Offline is enabled.")
	} else {

		// Check if we need to enable the live stream
		if config.Capture.Liveview != "false" {

			deviceId := config.Key
			hubKey := ""
			if config.Cloud == "s3" && config.S3 != nil && config.S3.Publickey != "" {
				hubKey = config.S3.Publickey
			} else if config.Cloud == "kstorage" && config.KStorage != nil && config.KStorage.CloudKey != "" {
				hubKey = config.KStorage.CloudKey
			}
			// This is the new way ;)
			if config.HubKey != "" {
				hubKey = config.HubKey
			}

			lastLivestreamRequestMQTT := int64(0)
			lastLivestreamRequestHTTP := int64(0)

			// HTTP transport (preferred when this agent is paired with a Kerberos
			// Hub): ship preview frames to hub-api over HTTPS instead of pushing
			// (large, base64) images through the MQTT broker. Viewers opt in per
			// session via the "http" transport on their keepalive; the legacy MQTT
			// push is kept for viewers (older frontends) that don't, and as a fallback.
			region := ""
			if config.S3 != nil {
				region = config.S3.Region
			}
			var snapshotPublisher *livesnapshot.Publisher
			if config.HubURI != "" && config.HubKey != "" {
				snapshotPublisher = livesnapshot.NewPublisher(livesnapshot.PublisherConfig{
					HubURI:        config.HubURI,
					HubKey:        config.HubKey,
					HubPrivateKey: config.HubPrivateKey,
					Region:        region,
					DeviceKey:     deviceId,
				})
				log.Log.Info("cloud.HandleLiveStreamSD(): HTTP preview transport ENABLED; frames go to " + strings.TrimRight(config.HubURI, "/") + "/storage/snapshot when a viewer requests it (kept off MQTT).")
			} else {
				log.Log.Info("cloud.HandleLiveStreamSD(): HTTP preview transport DISABLED (Hub not configured: HubURI/HubKey empty); preview frames are pushed over MQTT.")
			}

			// Track the transport actually used so we log only when it changes; the
			// loop runs once per keyframe and logging every frame would be noise.
			lastTransport := ""

			var cursorError error
			var pkt packets.Packet

			for cursorError == nil {
				pkt, cursorError = livestreamCursor.ReadPacket()
				if len(pkt.Data) == 0 || !pkt.IsKeyFrame {
					continue
				}
				now := time.Now().Unix()
				// Drain both viewer keepalive channels (non-blocking): one for the
				// HTTP transport, one for the legacy MQTT push.
				select {
				case <-communication.HandleLiveSD:
					lastLivestreamRequestMQTT = now
				default:
				}
				select {
				case <-communication.HandleLiveSDHTTP:
					lastLivestreamRequestHTTP = now
				default:
				}

				mqttViewerActive := now-lastLivestreamRequestMQTT <= 3
				httpViewerActive := now-lastLivestreamRequestHTTP <= 3
				if !mqttViewerActive && !httpViewerActive {
					continue
				}

				img, err := rtspClient.DecodePacket(pkt)
				if err != nil {
					continue
				}
				imageResized, _ := utils.ResizeImage(&img, uint(config.Capture.IPCamera.BaseWidth), uint(config.Capture.IPCamera.BaseHeight))
				bytes, _ := utils.ImageToBytes(imageResized)

				// Prefer HTTP for viewers that asked for it. Only if that did not
				// deliver (Hub not configured, or the upload failed) do we also push
				// over MQTT, so a new frontend can still fall back to its MQTT path.
				httpPushed := false
				var httpErr error
				if httpViewerActive && snapshotPublisher != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
					httpErr = snapshotPublisher.PublishSnapshot(ctx, bytes)
					if httpErr == nil {
						httpPushed = true
					}
					cancel()
				}

				pushMQTT := mqttViewerActive || (httpViewerActive && !httpPushed)

				// Log only when the effective transport changes, so an operator can
				// tell at a glance whether a device's preview travels over HTTP or
				// MQTT (and why it fell back) without per-frame log spam.
				transport := ""
				if httpPushed {
					transport = "http"
				} else if pushMQTT {
					transport = "mqtt"
				}
				if transport != "" && transport != lastTransport {
					if transport == "http" {
						log.Log.Info("cloud.HandleLiveStreamSD(): delivering preview frames over HTTP for device " + deviceId + ".")
					} else {
						reason := "viewer requested MQTT (older frontend)"
						if httpViewerActive && snapshotPublisher == nil {
							reason = "viewer asked for HTTP but Hub is not configured"
						} else if httpViewerActive && httpErr != nil {
							reason = "HTTP upload failed, falling back: " + httpErr.Error()
						}
						log.Log.Info("cloud.HandleLiveStreamSD(): delivering preview frames over MQTT for device " + deviceId + " (" + reason + ").")
					}
					lastTransport = transport
				}

				if pushMQTT {
					log.Log.Debug("cloud.HandleLiveStreamSD(): Sending base64 encoded images to MQTT.")
					chunking := config.Capture.LiveviewChunking

					if chunking == "true" {

						// Split encoded image into chunks of 2kb
						// This is to prevent the MQTT message to be too large.
						// By default, bytes are not encoded to base64 here; you are splitting the raw JPEG/PNG bytes.
						// However, in MQTT and web contexts, binary data may not be handled well, so base64 is often used.
						// To avoid base64 encoding, just send the raw []byte chunks as you do here.
						// If you want to avoid base64, make sure the receiver can handle binary payloads.

						chunkSize := 25 * 1024 // 25KB chunks
						var chunks [][]byte
						for i := 0; i < len(bytes); i += chunkSize {
							end := i + chunkSize
							if end > len(bytes) {
								end = len(bytes)
							}
							chunk := bytes[i:end]
							chunks = append(chunks, chunk)
						}

						log.Log.Infof("cloud.HandleLiveStreamSD(): Sending %d chunks of size %d bytes.", len(chunks), chunkSize)

						timestamp := time.Now().Unix()
						for i, chunk := range chunks {
							valueMap := make(map[string]interface{})
							valueMap["id"] = timestamp
							valueMap["chunk"] = chunk
							valueMap["chunkIndex"] = i
							valueMap["chunkSize"] = chunkSize
							valueMap["chunkCount"] = len(chunks)
							message := models.Message{
								Payload: models.Payload{
									Version:  "v1.0.0",
									Action:   "receive-sd-stream",
									DeviceId: deviceId,
									Value:    valueMap,
								},
							}
							payload, err := models.PackageMQTTMessage(configuration, message)
							if err == nil {
								mqttClient.Publish("kerberos/hub/"+hubKey+"/"+deviceId, 1, false, payload)
								log.Log.Infof("cloud.HandleLiveStreamSD(): sent chunk %d/%d to MQTT topic kerberos/hub/%s/%s", i+1, len(chunks), hubKey, deviceId)
								time.Sleep(33 * time.Millisecond) // Sleep to avoid flooding the MQTT broker with messages
							} else {
								log.Log.Info("cloud.HandleLiveStreamSD(): something went wrong while sending acknowledge config to hub: " + string(payload))
							}
						}
					} else {

						valueMap := make(map[string]interface{})
						valueMap["image"] = bytes
						message := models.Message{
							Payload: models.Payload{
								Action:   "receive-sd-stream",
								DeviceId: configuration.Config.Key,
								Value:    valueMap,
							},
						}
						payload, err := models.PackageMQTTMessage(configuration, message)
						if err == nil {
							mqttClient.Publish("kerberos/hub/"+hubKey, 0, false, payload)
						} else {
							log.Log.Info("cloud.HandleLiveStreamSD(): something went wrong while sending acknowledge config to hub: " + string(payload))
						}

					}
				}
				time.Sleep(1000 * time.Millisecond) // Sleep to avoid flooding the MQTT broker with messages
			}

		} else {
			log.Log.Debug("cloud.HandleLiveStreamSD(): stopping as Liveview is disabled.")
		}
	}

	log.Log.Debug("cloud.HandleLiveStreamSD(): finished")
}

func HandleLiveStreamHD(configuration *models.Configuration, communication *models.Communication, mqttClient mqtt.Client, rtspClient capture.RTSPClient, rtspSubClient capture.RTSPClient, subStreamEnabled bool) {

	config := configuration.Config

	if config.Offline == "true" {
		log.Log.Debug("cloud.HandleLiveStreamHD(): stopping as Offline is enabled.")
	} else {

		// Check if we need to enable the live stream
		if config.Capture.Liveview != "false" {

			// Create per-peer broadcasters instead of shared tracks.
			// Each viewer gets its own track with independent, non-blocking writes
			// so a slow/congested peer cannot stall the others.
			//
			// Both the main (high-resolution) and sub (low-resolution) streams are
			// exposed as separate broadcasters that are always forwarding, so a
			// viewer can pick the resolution it needs per peer connection without
			// the agent re-negotiating the RTSP source.
			mainStreams, _ := rtspClient.GetStreams()
			mainVideoBroadcaster := webrtc.NewVideoBroadcaster(mainStreams)
			mainAudioBroadcaster := webrtc.NewAudioBroadcaster(mainStreams)

			if mainVideoBroadcaster == nil && mainAudioBroadcaster == nil {
				log.Log.Error("cloud.HandleLiveStreamHD(): failed to create both video and audio broadcasters for the main stream")
				return
			}

			go webrtc.WriteToTrack(communication.Queue.Latest(), configuration, communication, mqttClient, mainVideoBroadcaster, mainAudioBroadcaster, rtspClient)

			// Sub stream broadcasters, only when a distinct sub stream is available.
			var subVideoBroadcaster *webrtc.TrackBroadcaster
			var subAudioBroadcaster *webrtc.TrackBroadcaster
			if subStreamEnabled && rtspSubClient != nil && communication.SubQueue != nil {
				subStreams, _ := rtspSubClient.GetStreams()
				subVideoBroadcaster = webrtc.NewVideoBroadcaster(subStreams)
				subAudioBroadcaster = webrtc.NewAudioBroadcaster(subStreams)
				go webrtc.WriteToTrack(communication.SubQueue.Latest(), configuration, communication, mqttClient, subVideoBroadcaster, subAudioBroadcaster, rtspSubClient)
			}
			subBroadcastersReady := subVideoBroadcaster != nil || subAudioBroadcaster != nil

			if config.Capture.ForwardWebRTC == "true" {

			} else {
				log.Log.Info("cloud.HandleLiveStreamHD(): Waiting for peer connections.")
				for handshake := range communication.HandleLiveHDHandshake {
					// Route each viewer to the main or sub broadcasters based on the
					// quality it requested; "auto" prefers the sub stream when one is
					// available, matching the historical default.
					useSub := models.SelectSubStreamForQuality(config, handshake.Payload.Quality, subStreamEnabled && subBroadcastersReady)
					videoBroadcaster := mainVideoBroadcaster
					audioBroadcaster := mainAudioBroadcaster
					streamLabel := "main"
					if useSub {
						videoBroadcaster = subVideoBroadcaster
						audioBroadcaster = subAudioBroadcaster
						streamLabel = "sub"
					}
					log.Log.Info("cloud.HandleLiveStreamHD(): setting up a peer connection on the " + streamLabel + " stream (quality=" + handshake.Payload.Quality + ").")
					go webrtc.InitializeWebRTCConnection(configuration, communication, mqttClient, videoBroadcaster, audioBroadcaster, handshake)
				}
			}

		} else {
			log.Log.Debug("cloud.HandleLiveStreamHD(): stopping as Liveview is disabled.")
		}
	}
}

func HandleRealtimeProcessing(processingCursor *packets.QueueCursor, configuration *models.Configuration, communication *models.Communication, mqttClient mqtt.Client, rtspClient capture.RTSPClient) {

	log.Log.Debug("cloud.RealtimeProcessing(): started")

	config := configuration.Config

	// If offline made is enabled, we will stop the thread.
	if config.Offline == "true" {
		log.Log.Debug("cloud.RealtimeProcessing(): stopping as Offline is enabled.")
	} else {

		// Check if we need to enable the realtime processing
		if config.RealtimeProcessing == "true" {

			hubKey := ""
			if config.Cloud == "s3" && config.S3 != nil && config.S3.Publickey != "" {
				hubKey = config.S3.Publickey
			} else if config.Cloud == "kstorage" && config.KStorage != nil && config.KStorage.CloudKey != "" {
				hubKey = config.KStorage.CloudKey
			}
			// This is the new way ;)
			if config.HubKey != "" {
				hubKey = config.HubKey
			}

			// We will publish the keyframes to the MQTT topic.
			realtimeProcessingTopic := "kerberos/keyframes/" + hubKey
			if config.RealtimeProcessingTopic != "" {
				realtimeProcessingTopic = config.RealtimeProcessingTopic
			}

			var cursorError error
			var pkt packets.Packet

			for cursorError == nil {
				pkt, cursorError = processingCursor.ReadPacket()
				if len(pkt.Data) == 0 || !pkt.IsKeyFrame {
					continue
				}

				log.Log.Info("cloud.RealtimeProcessing(): Sending base64 encoded images to MQTT.")
				img, err := rtspClient.DecodePacket(pkt)
				if err == nil {
					imageResized, _ := utils.ResizeImage(&img, uint(config.Capture.IPCamera.BaseWidth), uint(config.Capture.IPCamera.BaseHeight))
					bytes, _ := utils.ImageToBytes(imageResized)
					encoded := base64.StdEncoding.EncodeToString(bytes)

					valueMap := make(map[string]interface{})
					valueMap["image"] = encoded
					message := models.Message{
						Payload: models.Payload{
							Action:   "receive-keyframe",
							DeviceId: configuration.Config.Key,
							Value:    valueMap,
						},
					}
					payload, err := models.PackageMQTTMessage(configuration, message)
					if err == nil {
						mqttClient.Publish(realtimeProcessingTopic, 0, false, payload)
					} else {
						log.Log.Info("cloud.RealtimeProcessing(): something went wrong while sending acknowledge config to hub: " + string(payload))
					}
				}
			}

		} else {
			log.Log.Debug("cloud.RealtimeProcessing(): stopping as Liveview is disabled.")
		}
	}

	log.Log.Debug("cloud.HandleLiveStreamSD(): finished")
}

// VerifyHub godoc
// @Router /api/hub/verify [post]
// @ID verify-hub
// @Security Bearer
// @securityDefinitions.apikey Bearer
// @in header
// @name Authorization
// @Tags persistence
// @Param config body models.Config true "Config"
// @Summary Will verify the hub connectivity.
// @Description Will verify the hub connectivity.
// @Success 200 {object} models.APIResponse
func VerifyHub(c *gin.Context) {

	var config models.Config
	err := c.BindJSON(&config)

	if err == nil {
		hubURI := config.HubURI
		publicKey := config.HubKey
		privateKey := config.HubPrivateKey

		req, err := http.NewRequest("POST", hubURI+"/subscription/verify", nil)
		if err == nil {
			req.Header.Set("X-Kerberos-Hub-PublicKey", publicKey)
			req.Header.Set("X-Kerberos-Hub-PrivateKey", privateKey)
			var client *http.Client
			if os.Getenv("AGENT_TLS_INSECURE") == "true" {
				tr := &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				}
				client = &http.Client{Transport: tr}
			} else {
				client = &http.Client{}
			}

			resp, err := client.Do(req)
			if err == nil {
				body, err := io.ReadAll(resp.Body)
				defer resp.Body.Close()
				if err == nil {
					if resp.StatusCode == 200 {
						c.JSON(200, body)
					} else {
						c.JSON(400, models.APIResponse{
							Data: "cloud.VerifyHub(): something went wrong while reaching the Kerberos Hub API: " + string(body),
						})
					}
				} else {
					c.JSON(400, models.APIResponse{
						Data: "cloud.VerifyHub(): something went wrong while ready the response body: " + err.Error(),
					})
				}
			} else {
				c.JSON(400, models.APIResponse{
					Data: "cloud.VerifyHub(): something went wrong while reaching to the Kerberos Hub API: " + hubURI,
				})
			}
		} else {
			c.JSON(400, models.APIResponse{
				Data: "cloud.VerifyHub(): something went wrong while creating the HTTP request: " + err.Error(),
			})
		}
	} else {
		c.JSON(400, models.APIResponse{
			Data: "cloud.VerifyHub(): something went wrong while receiving the config " + err.Error(),
		})
	}
}

// VerifyPersistence godoc
// @Router /api/persistence/verify [post]
// @ID verify-persistence
// @Security Bearer
// @securityDefinitions.apikey Bearer
// @in header
// @name Authorization
// @Tags persistence
// @Param config body models.Config true "Config"
// @Summary Will verify the persistence.
// @Description Will verify the persistence.
// @Success 200 {object} models.APIResponse
func VerifyPersistence(c *gin.Context, configDirectory string) {

	var config models.Config
	err := c.BindJSON(&config)
	if err != nil || config.Cloud != "" {

		if config.Cloud == "dropbox" {
			VerifyDropbox(config, c)
		} else if config.Cloud == "s3" || config.Cloud == "kerberoshub" {

			if config.HubURI == "" ||
				config.HubKey == "" ||
				config.HubPrivateKey == "" ||
				config.S3.Region == "" {
				msg := "cloud.VerifyPersistence(kerberoshub): Kerberos Hub not properly configured."
				log.Log.Error(msg)
				c.JSON(400, models.APIResponse{
					Data: msg,
				})
			} else {

				// Open test-480p.mp4
				file, err := os.Open(configDirectory + "/data/test-480p.mp4")
				if err != nil {
					msg := "cloud.VerifyPersistence(kerberoshub): error reading test-480p.mp4: " + err.Error()
					log.Log.Error(msg)
					c.JSON(400, models.APIResponse{
						Data: msg,
					})
				}
				defer file.Close()

				req, err := http.NewRequest("POST", config.HubURI+"/storage/upload", file)
				if err != nil {
					msg := "cloud.VerifyPersistence(kerberoshub): error reading Kerberos Hub HEAD request, " + config.HubURI + "/storage: " + err.Error()
					log.Log.Error(msg)
					c.JSON(400, models.APIResponse{
						Data: msg,
					})
				}

				timestamp := time.Now().Unix()
				fileName := strconv.FormatInt(timestamp, 10) +
					"_6-967003_" + config.Name + "_200-200-400-400_24_769.mp4"
				req.Header.Set("X-Kerberos-Storage-FileName", fileName)
				req.Header.Set("X-Kerberos-Storage-Capture", "IPCamera")
				req.Header.Set("X-Kerberos-Storage-Device", config.Key)
				req.Header.Set("X-Kerberos-Hub-PublicKey", config.HubKey)
				req.Header.Set("X-Kerberos-Hub-PrivateKey", config.HubPrivateKey)
				req.Header.Set("X-Kerberos-Hub-Region", config.S3.Region)

				var client *http.Client
				if os.Getenv("AGENT_TLS_INSECURE") == "true" {
					tr := &http.Transport{
						TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
					}
					client = &http.Client{Transport: tr}
				} else {
					client = &http.Client{}
				}

				resp, err := client.Do(req)
				if resp != nil {
					defer resp.Body.Close()
				}

				if err == nil && resp != nil {
					if resp.StatusCode == 200 {
						msg := "cloud.VerifyPersistence(kerberoshub): Upload allowed using the credentials provided (" + config.HubKey + ", " + config.HubPrivateKey + ")"
						log.Log.Info(msg)
						c.JSON(200, models.APIResponse{
							Data: msg,
						})
					} else {
						msg := "cloud.VerifyPersistence(kerberoshub): Upload NOT allowed using the credentials provided (" + config.HubKey + ", " + config.HubPrivateKey + ")"
						log.Log.Error(msg)
						c.JSON(400, models.APIResponse{
							Data: msg,
						})
					}
				} else {
					msg := "cloud.VerifyPersistence(kerberoshub): Error creating Kerberos Hub request"
					log.Log.Error(msg)
					c.JSON(400, models.APIResponse{
						Data: msg,
					})
				}
			}

		} else if config.Cloud == "kstorage" || config.Cloud == "kerberosvault" {

			uri := config.KStorage.URI
			accessKey := config.KStorage.AccessKey
			secretAccessKey := config.KStorage.SecretAccessKey
			directory := config.KStorage.Directory
			provider := config.KStorage.Provider

			if err == nil && uri != "" && accessKey != "" && secretAccessKey != "" {

				var client *http.Client
				if os.Getenv("AGENT_TLS_INSECURE") == "true" {
					tr := &http.Transport{
						TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
					}
					client = &http.Client{Transport: tr}
				} else {
					client = &http.Client{}
				}

				req, err := http.NewRequest("POST", uri+"/ping", nil)
				if err == nil {
					req.Header.Add("X-Kerberos-Storage-AccessKey", accessKey)
					req.Header.Add("X-Kerberos-Storage-SecretAccessKey", secretAccessKey)
					resp, err := client.Do(req)

					if err == nil {
						body, err := io.ReadAll(resp.Body)
						defer resp.Body.Close()
						if err == nil && resp.StatusCode == http.StatusOK {

							if provider != "" || directory != "" {

								// Generate a random name.
								timestamp := time.Now().Unix()
								fileName := strconv.FormatInt(timestamp, 10) +
									"_6-967003_" + config.Name + "_200-200-400-400_24_769.mp4"

								// Open test-480p.mp4
								file, err := os.Open(configDirectory + "/data/test-480p.mp4")
								if err != nil {
									msg := "cloud.VerifyPersistence(kerberosvault): error reading test-480p.mp4: " + err.Error()
									log.Log.Error(msg)
									c.JSON(400, models.APIResponse{
										Data: msg,
									})
								}
								defer file.Close()

								req, err := http.NewRequest("POST", uri+"/storage", file)
								if err == nil {

									req.Header.Set("Content-Type", "video/mp4")
									req.Header.Set("X-Kerberos-Storage-CloudKey", config.HubKey)
									req.Header.Set("X-Kerberos-Storage-AccessKey", accessKey)
									req.Header.Set("X-Kerberos-Storage-SecretAccessKey", secretAccessKey)
									req.Header.Set("X-Kerberos-Storage-Provider", provider)
									req.Header.Set("X-Kerberos-Storage-FileName", fileName)
									req.Header.Set("X-Kerberos-Storage-Device", config.Key)
									req.Header.Set("X-Kerberos-Storage-Capture", "IPCamera")
									req.Header.Set("X-Kerberos-Storage-Directory", directory)

									var client *http.Client
									if os.Getenv("AGENT_TLS_INSECURE") == "true" {
										tr := &http.Transport{
											TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
										}
										client = &http.Client{Transport: tr}
									} else {
										client = &http.Client{}
									}

									resp, err := client.Do(req)

									if err == nil {
										if resp != nil {
											body, err := io.ReadAll(resp.Body)
											defer resp.Body.Close()
											if err == nil {
												if resp.StatusCode == 200 {
													msg := "cloud.VerifyPersistence(kerberosvault): Upload allowed using the credentials provided (" + accessKey + ", " + secretAccessKey + ")"
													log.Log.Info(msg)
													c.JSON(200, models.APIResponse{
														Data: body,
													})
												} else {
													msg := "cloud.VerifyPersistence(kerberosvault): Something went wrong while verifying your persistence settings. Make sure your provider is the same as the storage provider in your Kerberos Vault, and the relevant storage provider is configured properly."
													log.Log.Error(msg)
													c.JSON(400, models.APIResponse{
														Data: msg,
													})
												}
											}
										}
									} else {
										msg := "cloud.VerifyPersistence(kerberosvault): Upload of fake recording failed: " + err.Error()
										log.Log.Error(msg)
										c.JSON(400, models.APIResponse{
											Data: msg,
										})
									}
								} else {
									msg := "cloud.VerifyPersistence(kerberosvault): Something went wrong while creating /storage POST request." + err.Error()
									log.Log.Error(msg)
									c.JSON(400, models.APIResponse{
										Data: msg,
									})
								}
							} else {
								msg := "cloud.VerifyPersistence(kerberosvault): Provider and/or directory is missing from the request."
								log.Log.Error(msg)
								c.JSON(400, models.APIResponse{
									Data: msg,
								})
							}
						} else {
							msg := "cloud.VerifyPersistence(kerberosvault): Something went wrong while verifying storage credentials: " + string(body)
							log.Log.Error(msg)
							c.JSON(400, models.APIResponse{
								Data: msg,
							})
						}
					} else {
						msg := "cloud.VerifyPersistence(kerberosvault): Something went wrong while verifying storage credentials:" + err.Error()
						log.Log.Error(msg)
						c.JSON(400, models.APIResponse{
							Data: msg,
						})
					}
				} else {
					msg := "cloud.VerifyPersistence(kerberosvault): Something went wrong while verifying storage credentials:" + err.Error()
					log.Log.Error(msg)
					c.JSON(400, models.APIResponse{
						Data: msg,
					})
				}
			} else {
				msg := "cloud.VerifyPersistence(kerberosvault): please fill-in the required Kerberos Vault credentials."
				log.Log.Error(msg)
				c.JSON(400, models.APIResponse{
					Data: msg,
				})
			}
		}
	} else {
		msg := "cloud.VerifyPersistence(): No persistence was specified, so do not know what to verify:" + err.Error()
		log.Log.Error(msg)
		c.JSON(400, models.APIResponse{
			Data: msg,
		})
	}
}

// VerifySecondaryPersistence godoc
// @Router /api/persistence/secondary/verify [post]
// @ID verify-persistence
// @Security Bearer
// @securityDefinitions.apikey Bearer
// @in header
// @name Authorization
// @Tags persistence
// @Param config body models.Config true "Config"
// @Summary Will verify the secondary persistence.
// @Description Will verify the secondary persistence.
// @Success 200 {object} models.APIResponse
func VerifySecondaryPersistence(c *gin.Context, configDirectory string) {

	var config models.Config
	err := c.BindJSON(&config)
	if err != nil || config.Cloud != "" {

		if config.Cloud == "kstorage" || config.Cloud == "kerberosvault" {

			if config.KStorageSecondary == nil {
				msg := "cloud.VerifySecondaryPersistence(kerberosvault): please fill-in the required Kerberos Vault credentials."
				log.Log.Error(msg)
				c.JSON(400, models.APIResponse{
					Data: msg,
				})

			} else {

				uri := config.KStorageSecondary.URI
				accessKey := config.KStorageSecondary.AccessKey
				secretAccessKey := config.KStorageSecondary.SecretAccessKey
				directory := config.KStorageSecondary.Directory
				provider := config.KStorageSecondary.Provider

				if err == nil && uri != "" && accessKey != "" && secretAccessKey != "" {

					var client *http.Client
					if os.Getenv("AGENT_TLS_INSECURE") == "true" {
						tr := &http.Transport{
							TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
						}
						client = &http.Client{Transport: tr}
					} else {
						client = &http.Client{}
					}

					req, err := http.NewRequest("POST", uri+"/ping", nil)
					if err == nil {
						req.Header.Add("X-Kerberos-Storage-AccessKey", accessKey)
						req.Header.Add("X-Kerberos-Storage-SecretAccessKey", secretAccessKey)
						resp, err := client.Do(req)

						if err == nil {
							body, err := io.ReadAll(resp.Body)
							defer resp.Body.Close()
							if err == nil && resp.StatusCode == http.StatusOK {

								if provider != "" || directory != "" {

									// Generate a random name.
									timestamp := time.Now().Unix()
									fileName := strconv.FormatInt(timestamp, 10) +
										"_6-967003_" + config.Name + "_200-200-400-400_24_769.mp4"

									// Open test-480p.mp4
									file, err := os.Open(configDirectory + "/data/test-480p.mp4")
									if err != nil {
										msg := "cloud.VerifyPersistence(kerberosvault): error reading test-480p.mp4: " + err.Error()
										log.Log.Error(msg)
										c.JSON(400, models.APIResponse{
											Data: msg,
										})
									}
									defer file.Close()

									req, err := http.NewRequest("POST", uri+"/storage", file)
									if err == nil {

										req.Header.Set("Content-Type", "video/mp4")
										req.Header.Set("X-Kerberos-Storage-CloudKey", config.HubKey)
										req.Header.Set("X-Kerberos-Storage-AccessKey", accessKey)
										req.Header.Set("X-Kerberos-Storage-SecretAccessKey", secretAccessKey)
										req.Header.Set("X-Kerberos-Storage-Provider", provider)
										req.Header.Set("X-Kerberos-Storage-FileName", fileName)
										req.Header.Set("X-Kerberos-Storage-Device", config.Key)
										req.Header.Set("X-Kerberos-Storage-Capture", "IPCamera")
										req.Header.Set("X-Kerberos-Storage-Directory", directory)

										var client *http.Client
										if os.Getenv("AGENT_TLS_INSECURE") == "true" {
											tr := &http.Transport{
												TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
											}
											client = &http.Client{Transport: tr}
										} else {
											client = &http.Client{}
										}

										resp, err := client.Do(req)

										if err == nil {
											if resp != nil {
												body, err := io.ReadAll(resp.Body)
												defer resp.Body.Close()
												if err == nil {
													if resp.StatusCode == 200 {
														msg := "cloud.VerifySecondaryPersistence(kerberosvault): Upload allowed using the credentials provided (" + accessKey + ", " + secretAccessKey + ")"
														log.Log.Info(msg)
														c.JSON(200, models.APIResponse{
															Data: body,
														})
													} else {
														msg := "cloud.VerifySecondaryPersistence(kerberosvault): Something went wrong while verifying your persistence settings. Make sure your provider is the same as the storage provider in your Kerberos Vault, and the relevant storage provider is configured properly."
														log.Log.Error(msg)
														c.JSON(400, models.APIResponse{
															Data: msg,
														})
													}
												}
											}
										} else {
											msg := "cloud.VerifySecondaryPersistence(kerberosvault): Upload of fake recording failed: " + err.Error()
											log.Log.Error(msg)
											c.JSON(400, models.APIResponse{
												Data: msg,
											})
										}
									} else {
										msg := "cloud.VerifySecondaryPersistence(kerberosvault): Something went wrong while creating /storage POST request." + err.Error()
										log.Log.Error(msg)
										c.JSON(400, models.APIResponse{
											Data: msg,
										})
									}
								} else {
									msg := "cloud.VerifySecondaryPersistence(kerberosvault): Provider and/or directory is missing from the request."
									log.Log.Error(msg)
									c.JSON(400, models.APIResponse{
										Data: msg,
									})
								}
							} else {
								msg := "cloud.VerifySecondaryPersistence(kerberosvault): Something went wrong while verifying storage credentials: " + string(body)
								log.Log.Error(msg)
								c.JSON(400, models.APIResponse{
									Data: msg,
								})
							}
						} else {
							msg := "cloud.VerifySecondaryPersistence(kerberosvault): Something went wrong while verifying storage credentials:" + err.Error()
							log.Log.Error(msg)
							c.JSON(400, models.APIResponse{
								Data: msg,
							})
						}
					} else {
						msg := "cloud.VerifySecondaryPersistence(kerberosvault): Something went wrong while verifying storage credentials:" + err.Error()
						log.Log.Error(msg)
						c.JSON(400, models.APIResponse{
							Data: msg,
						})
					}
				} else {
					msg := "cloud.VerifySecondaryPersistence(kerberosvault): please fill-in the required Kerberos Vault credentials."
					log.Log.Error(msg)
					c.JSON(400, models.APIResponse{
						Data: msg,
					})
				}
			}
		}
	} else {
		msg := "cloud.VerifySecondaryPersistence(): No persistence was specified, so do not know what to verify:" + err.Error()
		log.Log.Error(msg)
		c.JSON(400, models.APIResponse{
			Data: msg,
		})
	}
}
