package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"github.com/gographics/imagick/imagick"
	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
	"io"
	"io/ioutil"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const DEFAULT_BUFFER_SIZE = 1024 * 64
const DEFAULT_UPLOAD_BUFFER_SIZE = 1024 * 1024 * 5
const DEFAULT_CORE_NUM = 4
const DEFAULT_COMPRESS_QUALITY = 50
const DEFAULT_THUMBNAIL_WIDTH = 200
const DEFAULT_STAT_SIZE = 10
const CACHE_CONTROL = "max-age=2629000: public"
const METHOD_GET = "GET"
const METHOD_POST = "POST"
const HEADER_UID = "uid"
const PARAMETER_FID = "fid"
const PARAMETER_UID = "uid"
const PARAMETER_UPLOAD = "upload"
const HEADER_CONTENT_TYPE = "Content-Type"
const HEADER_CONTENT_LENGTH = "Content-Length"
const HEADER_CONTENT_MD5 = "Content-MD5"
const HEADER_CACHE_CONTROL = "Cache-Control"
const CONTENT_TYPE_AUDIO_PREFIX = "audio"
const CONTENT_TYPE_VIDEO_PREFIX = "video"
const CONTENT_TYPE_IMAGE_PREFIX = "image"
const AWK_REGION = "us-east-1"

// file path and content
type Gridfile struct {
	Filename string
}

// config options to unmarshaled from json
type config struct {
	Servers          []string
	Logfile          string
	Database         string
	GridFSCollection string
	Listen           string
	HandlePath       string
	Debug            bool
	Mode             string
	CoreNum          int
}

// make gridfs, logger and config globally accessible
type gogridfs struct {
	Conf        config
	GFS         *mgo.GridFS
	Session     *mgo.Session
	Logger      *log.Logger
	ImageBucket *s3.Bucket
	AudioBucket *s3.Bucket
	VideoBucket *s3.Bucket
}

type UploadResult struct {
	Fid    string `json:"fid"`
	Result string `json:"result"`
}

var ggfs gogridfs

// load config
func loadConfig(config string) {
	b_file, err := ioutil.ReadFile(config)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(b_file, &ggfs.Conf)
	if err != nil {
		panic(err)
	}
}

// download file handler
func download(w http.ResponseWriter, r *http.Request) {
	// ignore invalid request
	if r.Method != METHOD_GET {
		ggfs.Logger.Println("[WARN] unsupport method", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	begin := time.Now().UnixNano()
	// extract header
	uid := r.Header.Get(HEADER_UID)
	// extract parameters
	parameters, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		ggfs.Logger.Println("[ERROR] error when parse parameter", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if len(parameters) == 0 {
		ggfs.Logger.Println("[ERROR] invalid query parameter")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	name := parameters[PARAMETER_FID][0]
	// get gridfs
	session := ggfs.Session.Copy()
	gridFS := session.DB(ggfs.Conf.Database).GridFS(ggfs.Conf.GridFSCollection)
	// open gridfile where path is the filename in GridFS
	file, err := gridFS.Open(name)
	if err != nil && err != mgo.ErrNotFound {
		ggfs.Logger.Println("[ERROR] error when open file", name, "from gridfs", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if file == nil {
		ggfs.Logger.Println("[WARN] file not found for given fid", name)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// set header
	// w.Header().Set(HEADER_CACHE_CONTROL, CACHE_CONTROL)
	w.Header().Set(HEADER_CONTENT_MD5, file.MD5())
	contentType := file.ContentType()
	if contentType != "" {
		w.Header().Set(HEADER_CONTENT_TYPE, contentType)
	}
	w.Header().Set(HEADER_CONTENT_LENGTH, strconv.FormatInt(file.Size(), 10))
	// read file into buffer
	var n int
	var buf = make([]byte, DEFAULT_BUFFER_SIZE)
	for {
		n, err = file.Read(buf)
		if n == 0 && err != nil {
			break
		} else {
			w.Write(buf[:n])
		}
	}
	if err != io.EOF {
		ggfs.Logger.Println("[ERROR] error when read file", name, "from chunk", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()
	defer session.Close()
	end := time.Now().UnixNano()
	if ggfs.Conf.Debug == true {
		ggfs.Logger.Println("[INFO] download", uid, name, (end-begin)/1000000)
	}
}

// thumbnail file handler
func thumbnail(w http.ResponseWriter, r *http.Request) {
	// ignore invalid request
	if r.Method != METHOD_GET {
		ggfs.Logger.Println("[WARN] unsupport method", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	begin := time.Now().UnixNano()
	// extract header
	uid := r.Header.Get(HEADER_UID)
	// extract parameters
	parameters, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		ggfs.Logger.Println("[ERROR] error when parse parameters", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if len(parameters) == 0 {
		ggfs.Logger.Println("[ERROR] invalid parameters")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	name := parameters[PARAMETER_FID][0]
	// get gridfs
	session := ggfs.Session.Copy()
	defer session.Close()
	gridFS := session.DB(ggfs.Conf.Database).GridFS(ggfs.Conf.GridFSCollection)
	// open gridfile where path is the filename in GridFS
	file, err := gridFS.Open(name)
	if err != nil && err != mgo.ErrNotFound {
		ggfs.Logger.Println("[ERROR] error when open file", name, "from gridfs", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if file == nil {
		ggfs.Logger.Println("[WARN] file not found for given fid", name)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer file.Close()
	// read file into buffer
	var n int
	var buf = make([]byte, DEFAULT_BUFFER_SIZE)
	var data bytes.Buffer
	for {
		n, err = file.Read(buf)
		if n == 0 && err != nil {
			break
		} else {
			data.Write(buf[:n])
		}
	}
	if err != io.EOF {
		ggfs.Logger.Println("[ERROR] error when read file", name, "from chunk", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	// set header
	// w.Header().Set(HEADER_CACHE_CONTROL, CACHE_CONTROL)
	w.Header().Set(HEADER_CONTENT_MD5, file.MD5())
	contentType := file.ContentType()
	if contentType != "" {
		w.Header().Set(HEADER_CONTENT_TYPE, contentType)
	}
	// make thumbnail
	mw := imagick.NewMagickWand()
	defer mw.Destroy()
	if len(data.Bytes()) == 0 {
		ggfs.Logger.Println("[ERROR] empty data to make thumbnail", name)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = mw.ReadImageBlob(data.Bytes())
	if err != nil {
		ggfs.Logger.Println("[ERROR] error when read file", "to make thumbnail", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	orginWidth := mw.GetImageWidth()
	orginHeight := mw.GetImageHeight()
	err = mw.ThumbnailImage(DEFAULT_THUMBNAIL_WIDTH, orginHeight*DEFAULT_THUMBNAIL_WIDTH/orginWidth)
	if err != nil {
		ggfs.Logger.Println("[ERROR] error when make thumbnail", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = mw.SetImageCompressionQuality(DEFAULT_COMPRESS_QUALITY)
	if err != nil {
		ggfs.Logger.Println("[ERROR] error when compress", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	thumbnailed := mw.GetImageBlob()
	w.Header().Set(HEADER_CONTENT_LENGTH, strconv.FormatInt(int64(len(thumbnailed)), 10))
	w.Write(thumbnailed)
	end := time.Now().UnixNano()
	if ggfs.Conf.Debug == true {
		ggfs.Logger.Println("[INFO] thumbnail", uid, name, (end-begin)/1000000)
	}
}

// upload file handler
func upload(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()
	// ignore invalid request
	if r.Method != METHOD_POST {
		ggfs.Logger.Println("[WARN] unsupport method", r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// extract header
	uid := r.Header.Get(HEADER_UID)
	parsedUid := int64(0)
	if uid != "" {
		parsedUid, _ = strconv.ParseInt(uid, 10, 64)
	}
	// extract multipart form
	r.ParseMultipartForm(DEFAULT_UPLOAD_BUFFER_SIZE)
	if r.MultipartForm == nil {
		ggfs.Logger.Println("[WARN] empty multipart form")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	file, _, err := r.FormFile(PARAMETER_UPLOAD)
	if err != nil {
		ggfs.Logger.Println("[ERROR] error when parse multipart form", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer file.Close()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		ggfs.Logger.Println("[ERROR] error when read uploaded file", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// generate fid
	length := int64(len(data))
	contentType := http.DetectContentType(data)
	name := strconv.FormatInt(begin.Unix()+parsedUid+length, 10)
	// upload to s3
	go toS3(name, contentType, data)
	// get gridfs
	session := ggfs.Session.Copy()
	gridFS := session.DB(ggfs.Conf.Database).GridFS(ggfs.Conf.GridFSCollection)
	gridFile, err := gridFS.Create(string(name))
	if err != nil {
		ggfs.Logger.Println("[ERROR] error when create file", name, "to gridfs", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if gridFile == nil {
		ggfs.Logger.Println("[ERROR] failed to create file to gridfs", name)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	gridFile.SetName(name)
	gridFile.SetContentType(contentType)
	gridFile.SetMeta(bson.M{"uid": uid})
	n, err := gridFile.Write(data)
	if err != nil {
		ggfs.Logger.Println("[ERROR] error when write to chunk", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer gridFile.Close()
	if int64(n) != length {
		ggfs.Logger.Println("[ERROR] write to chunk incompletely")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	end := time.Now().UnixNano()
	if ggfs.Conf.Debug == true {
		ggfs.Logger.Println("[INFO] upload", uid, name, contentType, length, (end-begin.UnixNano())/1000000)
	}
	result := UploadResult{Fid: name, Result: "OK"}
	response, _ := json.Marshal(result)
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

func toS3(name string, contentType string, data []byte) {
	begin := time.Now().UnixNano()
	if strings.HasPrefix(contentType, CONTENT_TYPE_AUDIO_PREFIX) {
		err := ggfs.AudioBucket.Put(name, data, contentType, s3.PublicRead)
		if err != nil {
			ggfs.Logger.Println("[ERROR] error when upload to s3", name, contentType, err)
		} else {
			end := time.Now().UnixNano()
			ggfs.Logger.Println("[INFO] upload to s3", name, contentType, (end-begin)/1000000)
		}
	} else if strings.HasPrefix(contentType, CONTENT_TYPE_VIDEO_PREFIX) {
		err := ggfs.VideoBucket.Put(name, data, contentType, s3.PublicRead)
		if err != nil {
			ggfs.Logger.Println("[ERROR] error when upload to s3", name, contentType, err)
		} else {
			end := time.Now().UnixNano()
			ggfs.Logger.Println("[INFO] upload to s3", name, contentType, (end-begin)/1000000)
		}
	} else if strings.HasPrefix(contentType, CONTENT_TYPE_IMAGE_PREFIX) {
		err := ggfs.ImageBucket.Put(name, data, contentType, s3.PublicRead)
		if err != nil {
			ggfs.Logger.Println("[ERROR] error when upload to s3", name, contentType, err)
		} else {
			end := time.Now().UnixNano()
			ggfs.Logger.Println("[INFO] upload to s3", name, contentType, (end-begin)/1000000)
		}
	} else {
		log.Println("[WARN] ignore due to invalid type", name, contentType)
	}
}

func main() {
	// get config file from command line args
	var config_file = flag.String("config", "config.json", "Config file in JSON format")
	flag.Parse()
	// load config from JSON file
	loadConfig(*config_file)
	// set processor num
	if ggfs.Conf.CoreNum == 0 {
		runtime.GOMAXPROCS(DEFAULT_CORE_NUM)
	} else {
		runtime.GOMAXPROCS(ggfs.Conf.CoreNum)
	}
	// initialize log writer
	var writer io.Writer
	if ggfs.Conf.Logfile == "" {
		writer = os.Stdout
	} else {
		writer, _ = os.OpenFile(ggfs.Conf.Logfile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	}
	ggfs.Logger = log.New(writer, "", log.Ldate|log.Ltime)
	// concatenate mongodb servers to single string of comma seperated servers
	var servers string
	for _, server := range ggfs.Conf.Servers {
		servers += (server + ",")
	}
	// die if no servers are configured
	if servers == "" {
		ggfs.Logger.Fatalln("[ERROR] invalid mongodb server.")
	}
	// determine mode, default is Strong
	// Strong (safe) => 2
	// Monotonic (fast) => 1
	// Eventual (faster) => 0
	mode := mgo.Strong
	if strings.ToLower(ggfs.Conf.Mode) == "monotonic" {
		mode = mgo.Monotonic
	} else if strings.ToLower(ggfs.Conf.Mode) == "eventual" {
		mode = mgo.Eventual
	}
	// connect to mongodb
	session, err := mgo.Dial(servers)
	session.SetMode(mode, true)
	if err != nil {
		ggfs.Logger.Fatalln("[ERROR] error when connect mongodb servers.", err)
	}
	ggfs.Session = session
	defer session.Close()
	// initialize imagemagick
	imagick.Initialize()
	defer imagick.Terminate()
	// initialize aws client
	auth, err := aws.GetAuth(AWS_ACCESS_KEY, AWS_SECRET_KEY)
	if err != nil {
		ggfs.Logger.Fatalln("[ERROR] error when initialize aws client", err)
	}
	client := s3.New(auth, aws.Regions[AWK_REGION])
	ggfs.ImageBucket = client.Bucket("mico-image")
	ggfs.VideoBucket = client.Bucket("mico-video")
	ggfs.AudioBucket = client.Bucket("mico-audio")
	// run webserver
	http.HandleFunc("/file/download", download)
	http.HandleFunc("/file/thumbnail", thumbnail)
	http.HandleFunc("/file/upload", upload)
	http.ListenAndServe(ggfs.Conf.Listen, nil)

	// mongoHandler := http.NewServeMux()
	// mongoHandler.HandleFunc("/file/download", download)
	// mongoHandler.HandleFunc("/file/thumbnail", thumbnail)
	// mongoHandler.HandleFunc("/file/upload", upload)
	// server := &http.Server{
	// 	Addr:           ggfs.Conf.Listen,
	// 	Handler:        mongoHandler,
	// 	ReadTimeout:    10 * time.Second,
	// 	WriteTimeout:   10 * time.Second,
	// 	MaxHeaderBytes: 16 * 1024,
	// }
	// fmt.Println(server.ListenAndServe())
}
