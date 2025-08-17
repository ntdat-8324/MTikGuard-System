import os
os.environ["CUDA_VISIBLE_DEVICES"] = "0"
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "max_split_size_mb:128"
import sys
import json
import logging
import traceback
from kafka import KafkaConsumer
from datetime import datetime
import torch
import torch.nn as nn
from dataclasses import dataclass
from transformers import AutoTokenizer, AutoModel, PreTrainedModel, PretrainedConfig
import cv2
import numpy as np
import gc
import easyocr
import av
import librosa
from PIL import Image
import re
import time
import random
from transformers import pipeline
from pymongo import MongoClient
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


# Cài đặt certifi nếu chưa có
try:
    import certifi
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "certifi"])
    import certifi

# --- TỰ ĐỘNG TẢI MODEL NẾU CHƯA CÓ ---
MODEL_DIR = "checkpoint"
MODEL_SUBDIR = "final-smart-model-fixed"
MODEL_ZIP = "model_checkpoint.zip"
MODEL_DRIVE_ID = "1tk8RtdMNYA0UnbvUJN9PxA7icLSEX0_k"
MODEL_PATH = os.path.join(MODEL_DIR, MODEL_SUBDIR)
if not os.path.exists(MODEL_PATH):
    try:
        import gdown
    except ImportError:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "gdown"])
        import gdown
    print("Downloading model checkpoint from Google Drive...")
    gdown.download(f"https://drive.google.com/uc?id={MODEL_DRIVE_ID}", MODEL_ZIP, quiet=False)
    import zipfile
    with zipfile.ZipFile(MODEL_ZIP, "r") as zip_ref:
        zip_ref.extractall(MODEL_DIR)
    print(f"Model extracted to {MODEL_DIR}")

# ===================== SETUP LOGGING =====================
def setup_logging():
    # Tạo thư mục logs nếu chưa có
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Tạo tên file log với timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'logs/consumer_atlas_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()
logger.info("🚀 Consumer Atlas khởi động...")
logger.info(f"📝 Log file: {os.path.abspath('logs')}")

# Kafka config
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'Test_with_MongoDB11'
GROUP_ID = 'tiktok_consumer_atlas_group'

# ===================== MONGODB ATLAS CONNECTION (SIMPLE, COMPASS-COMPATIBLE) =====================
def connect_mongodb_atlas_simple(connection_string):
    try:
        client = MongoClient(connection_string, server_api=ServerApi('1'))
        client.admin.command('ping')
        logger.info("✅ Kết nối MongoDB Atlas thành công!")
        return client
    except Exception as e:
        logger.error(f"❌ Không thể kết nối MongoDB Atlas: {e}")
        return None

# ===================== TEXT EXTRACTION CLASSES =====================
class AdvancedSpamFilter:
    def __init__(self):
        self.spam_patterns = [
            re.compile(r'.*hãy\s+(đăng\s*ký|subscribe).*kênh.*', re.IGNORECASE),
            re.compile(r'.*(đăng\s*ký|subscribe).*kênh.*', re.IGNORECASE),
            re.compile(r'.*subscribe.*cho.*kênh.*', re.IGNORECASE),
            re.compile(r'.*theo\s*dõi.*kênh.*', re.IGNORECASE),
            re.compile(r'.*nhấn.*đăng\s*ký.*kênh.*', re.IGNORECASE),
            re.compile(r'.*bấm.*đăng\s*ký.*kênh.*', re.IGNORECASE),
            re.compile(r'.*please\s+(subscribe|follow).*channel.*', re.IGNORECASE),
            re.compile(r'.*(subscribe|follow).*channel.*please.*', re.IGNORECASE),
            re.compile(r'.*hit.*subscribe.*channel.*', re.IGNORECASE),
            re.compile(r'.*click.*subscribe.*channel.*', re.IGNORECASE),
            re.compile(r'.*bấm\s+(like|chuông).*kênh.*', re.IGNORECASE),
            re.compile(r'.*bấm.*chuông.*thông.*báo.*kênh.*', re.IGNORECASE),
            re.compile(r'.*để\s+không\s+bỏ\s+lỡ.*video.*', re.IGNORECASE),
            re.compile(r'.*(like|chia\s*sẻ).*video.*kênh.*', re.IGNORECASE),
            re.compile(r'.*(like|share).*video.*channel.*', re.IGNORECASE),
            re.compile(r'.*hit.*notification.*bell.*', re.IGNORECASE),
            re.compile(r'.*don\'t\s+forget.*subscribe.*', re.IGNORECASE),
            re.compile(r'.*smash.*like.*subscribe.*', re.IGNORECASE),
            re.compile(r'.*ghiền\s+mì\s+gõ.*kênh.*', re.IGNORECASE),
            re.compile(r'.*(follow|theo\s*dõi).*(facebook|instagram|tiktok).*page.*', re.IGNORECASE),
            re.compile(r'.*subscribe.*and.*like.*channel.*', re.IGNORECASE),
            re.compile(r'.*like.*and.*subscribe.*channel.*', re.IGNORECASE),
            re.compile(r'.*support.*our.*channel.*', re.IGNORECASE),
            re.compile(r'.*ủng\s*hộ.*kênh.*của.*', re.IGNORECASE),
            re.compile(r'^[@#$%^&*()_+=\[\]{}|\\:";\'<>?,./\s]+$'),
            re.compile(r'^[0-9\s\-\.]+$'),
        ]
        self.extreme_gibberish_patterns = [
            re.compile(r'(.)\1{9,}'),
            re.compile(r'[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]{15,}'),
        ]
    def is_spam(self, text):
        if not text or len(text.strip()) < 1:
            return True
        text_clean = text.lower().strip()
        for pattern in self.spam_patterns:
            if pattern.search(text_clean):
                return True
        for pattern in self.extreme_gibberish_patterns:
            if pattern.search(text):
                return True
        return False
    def clean_repetition(self, text):
        if not text:
            return ""
        cleaned = re.sub(r'\b(\w+)(\s+\1){7,}\b', r'\1', text)
        cleaned = re.sub(r'\s{8,}', ' ', cleaned)
        cleaned = cleaned.strip()
        return cleaned
    def process_text(self, text):
        if not text:
            return ""
        cleaned = text.strip()
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = self.clean_repetition(cleaned)
        if self.is_spam(cleaned):
            return ""
        return cleaned

class WhisperHallucinationFilter:
    def __init__(self):
        self.hallucination_patterns = [
            re.compile(r'^thank\s+you\.?$', re.IGNORECASE),
            re.compile(r'^oh\s+thank\s+you\.?$', re.IGNORECASE),
            re.compile(r'^thank\s+you\.?\s+so\s+so\.?$', re.IGNORECASE),
            re.compile(r'^thank\s+you\.?\s+you\.?$', re.IGNORECASE),
            re.compile(r'^oh\.?$', re.IGNORECASE),
            re.compile(r'^you\.?$', re.IGNORECASE),
            re.compile(r'^bye\.?$', re.IGNORECASE),
            re.compile(r'^hello\.?$', re.IGNORECASE),
            re.compile(r'^hi\.?$', re.IGNORECASE),
            re.compile(r'^(oh\s+){2,}.*', re.IGNORECASE),
            re.compile(r'^(you\s+){2,}.*', re.IGNORECASE),
            re.compile(r'^(hey\s+){2,}.*', re.IGNORECASE),
            re.compile(r'^i\s+just\s+wanna\s+be\s+with\s+you\.?$', re.IGNORECASE),
            re.compile(r'^everybody\s+make\s+some\s+noise\.?$', re.IGNORECASE),
            re.compile(r'^everybody\s+make\s+some\s+noise\s+foreign\.?$', re.IGNORECASE),
            re.compile(r'^(oh\s+)?(bye|hello|hi)\.?$', re.IGNORECASE),
        ]
        self.low_confidence_short_phrases = [
            'thank you', 'oh', 'you', 'bye', 'hello', 'hi', 'hey'
        ]
        self.context_dependent_phrases = [
            'thank you', 'oh', 'you', 'bye', 'hello', 'hi'
        ]
    def is_likely_hallucination(self, text):
        if not text or len(text.strip()) < 1:
            return True
        text_clean = text.strip().lower()
        word_count = len(text_clean.split())
        for pattern in self.hallucination_patterns:
            if pattern.match(text_clean):
                return True
        if word_count <= 2:
            for phrase in self.low_confidence_short_phrases:
                if phrase in text_clean and len(text_clean) <= len(phrase) + 3:
                    return True
        if word_count == 1:
            if text_clean.rstrip('.!?') in ['oh', 'you', 'bye', 'hi', 'hey']:
                return True
        return False
    def filter_text(self, text):
        if not text:
            return ""
        if self.is_likely_hallucination(text):
            return ""
        return text

class FastLanguageDetector:
    @staticmethod
    def detect_language(text):
        if not text or len(text.strip()) < 1:
            return "other"
        text_lower = text.lower()
        vi_chars = len(re.findall(r'[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]', text_lower))
        vi_words = len(re.findall(r'\b(và|của|trong|với|từ|để|này|đó|có|là|được|một|tôi|bạn|anh|em|không|rất|cảm|ơn|tình|yêu|xin|chào|cám|chúc|mừng|được|làm|đi|về|gì|như|nào|khi|nếu|bây|giờ|hôm|nay|ngày|mai)\b', text_lower))
        en_words = len(re.findall(r'\b(the|and|for|are|but|not|you|all|can|this|that|with|from|they|have|love|see|now|time|thank|hello|yes|no|good|great|nice|well|very|much|more|come|go|know|want|need|help|please|sorry|ok|okay|what|when|where|why|how|who|will|would|could|should|may|might|must|do|does|did|am|is|was|were|been|being|has|had|having|get|got|give|take|make|made|say|said|tell|told|ask|think|feel|look|seems|try|work|play|use|used|like|want|need)\b', text_lower))
        other_asian = len(re.findall(r'[\u0E00-\u0E7F\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]', text))
        if other_asian > len(text) * 0.1:
            return "other"
        vi_score = vi_chars * 3 + vi_words * 4
        en_score = en_words * 3
        word_count = len(text.split())
        if word_count <= 3:
            if vi_chars > 0:
                return "vietnamese"
            elif vi_words > 0:
                return "vietnamese"
            elif en_words > 0:
                return "english"
            elif any(word in text_lower for word in ['thank', 'hello', 'yes', 'no', 'ok', 'good', 'bad', 'nice', 'great', 'wow', 'ah', 'um', 'hmm']):
                return "english"
            else:
                return "other"
        if vi_score > en_score and vi_score > 0:
            return "vietnamese"
        elif en_score > 0:
            return "english"
        elif vi_chars > 0:
            return "vietnamese"
        else:
            return "other"
    @staticmethod
    def is_target_language(text):
        detected = FastLanguageDetector.detect_language(text)
        return detected in ["vietnamese", "english"]

class FastOCRExtractor:
    def __init__(self):
        try:
            self.reader = easyocr.Reader(['vi', 'en'], gpu=torch.cuda.is_available(), verbose=False)
            self.available = True
        except Exception as e:
            self.reader = None
            self.available = False
        self.spam_filter = AdvancedSpamFilter()
    def extract_text_safe(self, video_path):
        if not self.available or not self.reader:
            return ""
        try:
            container = av.open(str(video_path))
            if not container.streams.video:
                container.close()
                return ""
            video_stream = container.streams.video[0]
            total_frames = max(video_stream.frames or 1000, 1000)
            frame_positions = [int(total_frames * pos) for pos in [0.3, 0.7]]
            all_texts = []
            current_frame = 0
            extracted = 0
            for frame in container.decode(video=0):
                if current_frame in frame_positions:
                    try:
                        frame_array = frame.to_ndarray(format="rgb24")
                        h, w = frame_array.shape[:2]
                        if max(h, w) > 640:
                            scale = 640 / max(h, w)
                            new_h, new_w = int(h * scale), int(w * scale)
                            img = Image.fromarray(frame_array)
                            img = img.resize((new_w, new_h), Image.LANCZOS)
                            frame_array = np.array(img)
                        results = self.reader.readtext(
                            frame_array, 
                            detail=0, 
                            width_ths=0.7, 
                            height_ths=0.7
                        )
                        for text in results:
                            if isinstance(text, str) and text.strip():
                                processed = self.spam_filter.process_text(text.strip())
                                if processed:
                                    if FastLanguageDetector.is_target_language(processed):
                                        all_texts.append(processed)
                        extracted += 1
                    except Exception as ocr_error:
                        continue
                current_frame += 1
                if extracted >= 2:
                    break
            container.close()
            if all_texts:
                unique_texts = list(dict.fromkeys(all_texts))
                combined = ' '.join(unique_texts)
                final = self.spam_filter.process_text(combined)
                if final and FastLanguageDetector.is_target_language(final):
                    return final
            return ""
        except Exception as e:
            return ""

class FastAudioExtractor:
    def __init__(self):
        try:
            self.pipe = pipeline(
                "automatic-speech-recognition",
                model="openai/whisper-large-v3-turbo",
                torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
                device="cuda" if torch.cuda.is_available() else "cpu",
                return_timestamps=False,
                chunk_length_s=30,
                stride_length_s=5
            )
            self.available = True
        except Exception as e:
            self.pipe = None
            self.available = False
        self.spam_filter = AdvancedSpamFilter()
        self.hallucination_filter = WhisperHallucinationFilter()
    def get_video_duration_safe(self, video_path):
        try:
            container = av.open(str(video_path))
            if container.streams.video:
                duration = float(container.duration) / av.time_base if container.duration else 0
                container.close()
                return min(duration, 60)
            container.close()
            return 0
        except:
            return 60
    def extract_audio_safe(self, video_path):
        try:
            video_duration = self.get_video_duration_safe(video_path)
            duration_to_load = min(video_duration, 60)
            if duration_to_load < 0.5:
                return None, 0
            audio, _ = librosa.load(
                str(video_path),
                sr=16000,
                duration=duration_to_load,
                mono=True,
                res_type='kaiser_fast'
            )
            if len(audio) < 0.5 * 16000:
                return None, 0
            rms = np.sqrt(np.mean(audio**2))
            if rms < 1e-6:
                return None, 0
            max_val = np.abs(audio).max()
            if max_val > 0:
                audio = audio / max_val * 0.8
            actual_duration = len(audio) / 16000
            return audio.astype(np.float32), actual_duration
        except:
            return None, 0
    def transcribe_text_safe(self, video_path):
        if not self.available or not self.pipe:
            return ""
        try:
            audio, duration = self.extract_audio_safe(video_path)
            if audio is None:
                return ""
            best_result = ""
            for language in ["vi", "en"]:
                try:
                    result = self.pipe(
                        audio,
                        generate_kwargs={
                            "language": language,
                            "task": "transcribe",
                            "temperature": 0.0,
                            "no_speech_threshold": 0.7,
                            "compression_ratio_threshold": 2.4,
                            "logprob_threshold": -1.0,
                            "no_repeat_ngram_size": 3,
                            "repetition_penalty": 1.2
                        }
                    )
                    text = result.get('text', '').strip() if isinstance(result, dict) else str(result).strip()
                    if text:
                        if self.hallucination_filter.is_likely_hallucination(text):
                            continue
                        processed = self.spam_filter.process_text(text)
                        if processed and len(processed) > len(best_result):
                            detected = FastLanguageDetector.detect_language(processed)
                            if detected in ["vietnamese", "english"]:
                                final_filtered = self.hallucination_filter.filter_text(processed)
                                if final_filtered:
                                    best_result = final_filtered
                                    if detected == "vietnamese" and len(final_filtered) > 5:
                                        break
                except Exception as whisper_error:
                    continue
            if best_result and FastLanguageDetector.is_target_language(best_result):
                return best_result
            else:
                return ""
        except Exception as e:
            return ""

class CompleteProcessor:
    def __init__(self):
        self.ocr_extractor = FastOCRExtractor()
        self.audio_extractor = FastAudioExtractor()
    def process_video_safe(self, video_record):
        video_path = video_record['video_path']
        audio_text = ""
        ocr_text = ""
        try:
            if self.audio_extractor.available:
                audio_text = self.audio_extractor.transcribe_text_safe(video_path)
            if self.ocr_extractor.available:
                ocr_text = self.ocr_extractor.extract_text_safe(video_path)
        except Exception as e:
            pass
        parts = []
        if audio_text:
            parts.append(f"AUDIO: {audio_text}")
        if ocr_text:
            parts.append(f"OCR: {ocr_text}")
        combined = " | ".join(parts) if parts else ""
        video_record['ocr_text'] = ocr_text
        video_record['audio_text'] = audio_text
        video_record['combined_text'] = combined
        return video_record

# ===================== MULTIMODAL MODEL CLASSES =====================
@dataclass
class FixedMultimodalConfig(PretrainedConfig):
    model_type = "fixed_multimodal"
    def __init__(
        self,
        num_classes: int = 4,
        text_model_name: str = "distilbert-base-multilingual-cased",
        video_model_name: str = "facebook/timesformer-base-finetuned-k400",
        text_hidden_size: int = 768,
        video_hidden_size: int = 768,
        fusion_hidden_size: int = 256,
        classifier_hidden_size: int = 128,
        max_frames: int = 8,
        image_size: int = 224,
        dropout: float = 0.1,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.num_classes = num_classes
        self.text_model_name = text_model_name
        self.video_model_name = video_model_name
        self.text_hidden_size = text_hidden_size
        self.video_hidden_size = video_hidden_size
        self.fusion_hidden_size = fusion_hidden_size
        self.classifier_hidden_size = classifier_hidden_size
        self.max_frames = max_frames
        self.image_size = image_size
        self.dropout = dropout

class FixedMultimodalModel(PreTrainedModel):
    config_class = FixedMultimodalConfig
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.text_encoder = AutoModel.from_pretrained(config.text_model_name)
        from transformers import TimesformerForVideoClassification, AutoImageProcessor
        self.video_encoder = TimesformerForVideoClassification.from_pretrained(
            config.video_model_name,
            num_labels=config.num_classes,
            ignore_mismatched_sizes=True
        )
        self.image_processor = AutoImageProcessor.from_pretrained(config.video_model_name)
        video_dim = self.video_encoder.config.hidden_size
        text_dim = self.text_encoder.config.hidden_size
        self.config.video_hidden_size = video_dim
        self.config.text_hidden_size = text_dim
        self.text_projection = nn.Sequential(
            nn.Linear(text_dim, config.fusion_hidden_size),
            nn.ReLU(),
            nn.Dropout(config.dropout)
        )
        self.video_projection = nn.Sequential(
            nn.Linear(video_dim, config.fusion_hidden_size),
            nn.ReLU(),
            nn.Dropout(config.dropout)
        )
        self.fusion = nn.Sequential(
            nn.Linear(config.fusion_hidden_size * 2, config.fusion_hidden_size),
            nn.LayerNorm(config.fusion_hidden_size),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.fusion_hidden_size, config.classifier_hidden_size),
            nn.LayerNorm(config.classifier_hidden_size),
            nn.ReLU(),
            nn.Dropout(config.dropout)
        )
        self.classifier = nn.Linear(config.classifier_hidden_size, config.num_classes)
    def forward(self, pixel_values, input_ids, attention_mask, labels=None, **kwargs):
        text_outputs = self.text_encoder(
            input_ids=input_ids,
            attention_mask=attention_mask
        )
        text_features = text_outputs.last_hidden_state[:, 0, :]
        text_features = self.text_projection(text_features)
        try:
            batch_size, channels, num_frames, height, width = pixel_values.shape
            pixel_values_reshaped = pixel_values.permute(0, 2, 1, 3, 4)
            video_outputs = self.video_encoder.timesformer(pixel_values_reshaped)
            video_features = video_outputs.last_hidden_state[:, 0]
        except Exception as e:
            batch_size = pixel_values.size(0)
            video_features = torch.zeros(
                batch_size, self.config.video_hidden_size,
                dtype=text_features.dtype, device=text_features.device
            )
        video_features = self.video_projection(video_features)
        combined_features = torch.cat([text_features, video_features], dim=1)
        fused_features = self.fusion(combined_features)
        logits = self.classifier(fused_features)
        loss = None
        if labels is not None:
            loss_fct = nn.CrossEntropyLoss(label_smoothing=0.1)
            loss = loss_fct(logits, labels)
        return {'loss': loss, 'logits': logits}

class FixedVideoLoader:
    def __init__(self, max_frames=8, image_size=224):
        self.max_frames = max_frames
        self.image_size = image_size
        self.video_mean = [0.485, 0.456, 0.406]
        self.video_std = [0.229, 0.224, 0.225]
    def load_video_for_timesformer(self, video_path: str) -> torch.Tensor:
        if not video_path or not os.path.exists(video_path):
            return self._create_dummy_video()
        try:
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                return self._create_dummy_video()
            frames = []
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            if total_frames <= 0:
                cap.release()
                return self._create_dummy_video()
            if total_frames < self.max_frames:
                indices = np.linspace(0, max(0, total_frames-1), self.max_frames, dtype=int)
            else:
                indices = np.linspace(0, total_frames-1, self.max_frames, dtype=int)
            for idx in indices:
                cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
                ret, frame = cap.read()
                if ret and frame is not None and frame.size > 0:
                    try:
                        frame = cv2.resize(frame, (self.image_size, self.image_size))
                        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        frame = frame.astype(np.float32) / 255.0
                        mean = np.array(self.video_mean, dtype=np.float32).reshape(1, 1, 3)
                        std = np.array(self.video_std, dtype=np.float32).reshape(1, 1, 3)
                        frame = (frame - mean) / std
                        frames.append(frame)
                    except:
                        frames.append(np.zeros((self.image_size, self.image_size, 3), dtype=np.float32))
                else:
                    frames.append(np.zeros((self.image_size, self.image_size, 3), dtype=np.float32))
            cap.release()
            while len(frames) < self.max_frames:
                frames.append(frames[-1] if frames else np.zeros((self.image_size, self.image_size, 3), dtype=np.float32))
            video_array = np.stack(frames[:self.max_frames])
            video_tensor = torch.from_numpy(video_array).float()
            video_tensor = video_tensor.permute(3, 0, 1, 2)
            return video_tensor
        except Exception as e:
            return self._create_dummy_video()
    def _create_dummy_video(self):
        return torch.zeros(3, self.max_frames, self.image_size, self.image_size, dtype=torch.float32)

# ===================== LOAD MODEL, TOKENIZER, VIDEO LOADER =====================
MULTIMODAL_MODEL_PATH = MODEL_PATH

device = "cuda" if torch.cuda.is_available() else "cpu"
multimodal_config = FixedMultimodalConfig.from_pretrained(MULTIMODAL_MODEL_PATH)
multimodal_model = FixedMultimodalModel.from_pretrained(MULTIMODAL_MODEL_PATH, config=multimodal_config).to(device)
multimodal_model.eval()
text_tokenizer = AutoTokenizer.from_pretrained(multimodal_config.text_model_name)
video_loader = FixedVideoLoader(max_frames=8, image_size=224)

id2label = {0: 'Adult Content', 1: 'Harmful Content', 2: 'Safe', 3: 'Suicide'}
label2id = {'Adult Content': 0, 'Harmful Content': 1, 'Safe': 2, 'Suicide': 3}

text_processor = CompleteProcessor()

TTSCRAPER_PATH = os.path.abspath(os.path.join(os.getcwd(), r'C:\Users\Admin\PycharmProjects\UIT-Projects\TikTok-Content-Scraper'))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../TikTok-Content-Scraper')))
try:
    from TT_Scraper import TT_Scraper
except ImportError as e:
    logger.error(f"Cannot import TT_Scraper. Please check that TikTok-Content-Scraper is present and sys.path is set correctly.")
    raise

# ===================== PIPELINE FUNCTIONS =====================
def download_tiktok_content(video_id, output_prefix="downloaded_videos"):
    tt = TT_Scraper(wait_time=0.3, output_files_fp=output_prefix)
    try:
        logger.info(f"🔄 Đang tải video/slides cho ID: {video_id}")
        tt.scrape_list(ids=[video_id], scrape_content=True)
        logger.info(f"✅ Đã tải video/slides cho ID: {video_id}")
        
        # Tìm files đã tải
        files = [f for f in os.listdir('.') if f.startswith(output_prefix) and video_id in f]
        if not files:
            logger.error(f"❌ Không tìm thấy file nào cho video_id {video_id} với prefix {output_prefix}")
            return None
        
        # Ưu tiên video files
        for f in files:
            if f.lower().endswith(('.mp4', '.mov', '.avi', '.mkv')):
                file_path = os.path.abspath(f)
                logger.info(f"📁 Tìm thấy video file: {file_path}")
                return file_path
        
        # Fallback to first file
        file_path = os.path.abspath(files[0])
        logger.info(f"📁 Sử dụng file: {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi tải video/slides cho ID {video_id}: {e}")
        logger.error(traceback.format_exc())
        return None

def preprocess_video(video_path):
    try:
        if os.path.isfile(video_path) and video_path.lower().endswith(('.mp4', '.mov', '.avi', '.mkv')):
            logger.info(f"🎥 File video đã sẵn sàng: {video_path}")
            return video_path
        
        # Xử lý slides (ảnh)
        if os.path.isfile(video_path) and video_path.lower().endswith(('.jpg', '.jpeg', '.png')):
            slide_dir = os.path.dirname(video_path)
        elif os.path.isdir(video_path):
            slide_dir = video_path
        else:
            logger.error(f"❌ Không xác định được kiểu file: {video_path}")
            return None
        
        # Tìm tất cả ảnh slides
        images = [os.path.join(slide_dir, f) for f in sorted(os.listdir(slide_dir)) 
                 if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        
        if not images:
            logger.error(f"❌ Không tìm thấy ảnh slides trong {slide_dir}")
            return None
        
        logger.info(f"🖼️ Tìm thấy {len(images)} ảnh slides, đang ghép thành video...")
        
        # Đọc frame đầu tiên để lấy kích thước
        frame = cv2.imread(images[0])
        if frame is None:
            logger.error(f"❌ Không thể đọc ảnh đầu tiên: {images[0]}")
            return None
            
        height, width, _ = frame.shape
        out_video_path = os.path.join(slide_dir, "slides_merged.mp4")
        
        # Tạo video writer
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(out_video_path, fourcc, 1, (width, height))
        
        if not out.isOpened():
            logger.error(f"❌ Không thể tạo video writer cho {out_video_path}")
            return None
        
        # Ghép các ảnh thành video
        for img_path in images:
            img = cv2.imread(img_path)
            if img is not None:
                img = cv2.resize(img, (width, height))
                out.write(img)
            else:
                logger.warning(f"⚠️ Không thể đọc ảnh: {img_path}")
        
        out.release()
        logger.info(f"✅ Đã ghép {len(images)} slides thành video: {out_video_path}")
        return out_video_path
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi tiền xử lý video {video_path}: {e}")
        logger.error(traceback.format_exc())
        return None

def classify_video_multimodal(video_path, combined_text):
    try:
        text_inputs = text_tokenizer(
            combined_text if combined_text else "",
            max_length=64,
            truncation=True,
            padding='max_length',
            return_tensors='pt'
        )
        video_tensor = video_loader.load_video_for_timesformer(video_path)
        inputs = {
            'pixel_values': video_tensor.unsqueeze(0).to(device),
            'input_ids': text_inputs['input_ids'].to(device),
            'attention_mask': text_inputs['attention_mask'].to(device)
        }
        with torch.no_grad():
            outputs = multimodal_model(**inputs)
            logits = outputs['logits']
            pred_id = int(torch.argmax(logits, dim=-1).cpu().numpy())
            pred_label = id2label.get(pred_id, "Unknown")
            score = float(torch.softmax(logits, dim=-1)[0, pred_id].cpu().numpy())
        torch.cuda.empty_cache()
        return {"label": pred_label, "score": score}
    except Exception as e:
        logger.error(f"Lỗi khi predict video multimodal {video_path}: {e}")
        logger.error(traceback.format_exc())
        return {"label": "Unknown", "score": 0.0}

def save_to_mongo_atlas(metadata, video_path, classification=None):
    doc = metadata.copy()
    doc['video_file'] = video_path
    if classification:
        doc['classification'] = classification
    doc['processed_at'] = datetime.utcnow()
    doc['atlas_processed'] = True  # Flag to identify Atlas-processed documents
    if 'video_id' in doc:
        doc['_id'] = doc['video_id']
    try:
        # Sử dụng biến mongo_collection toàn cục
        global mongo_collection
        mongo_collection.replace_one({'_id': doc['_id']}, doc, upsert=True)
        logger.info(f"✅ Đã lưu metadata + video vào MongoDB Atlas cho video {doc['_id']}.")
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi khi lưu vào MongoDB Atlas: {e}")
        logger.error(traceback.format_exc())
        return False

def cleanup_files(video_id, output_prefix="downloaded_videos"):
    files = [f for f in os.listdir('.') if f.startswith(output_prefix) and video_id in f]
    for f in files:
        try:
            os.remove(f)
            logger.info(f"🗑️ Đã xóa file: {f}")
        except Exception as e:
            logger.warning(f"Không thể xóa file {f}: {e}")

def log_mongo_atlas_count():
    try:
        global mongo_collection
        count = mongo_collection.count_documents({})
        atlas_count = mongo_collection.count_documents({"atlas_processed": True})
        logger.info(f"📊 Số lượng document trong MongoDB Atlas: {count} (Atlas-processed: {atlas_count})")
    except Exception as e:
        logger.warning(f"Không thể đếm document trong MongoDB Atlas: {e}")

# ===================== GLOBAL VARIABLES =====================
mongo_collection = None
mongo_client = None

# ===================== INITIALIZE MONGODB ATLAS CONNECTION =====================
def initialize_mongodb_atlas():
    global mongo_collection, mongo_client
    
    try:
        with open('uri_mongodb_atlas.txt', 'r', encoding='utf-8') as f:
            MONGO_ATLAS_URI = f.read().strip()
    except FileNotFoundError:
        logger.error("❌ File uri_mongodb_atlas.txt không tồn tại")
        return False
    except Exception as e:
        logger.error(f"❌ Failed to read MongoDB Atlas URI: {e}")
        return False
    
    mongo_client = connect_mongodb_atlas_simple(MONGO_ATLAS_URI)
    if not mongo_client:
        logger.error("❌ Failed to connect to MongoDB Atlas")
        return False
    
    mongo_db = mongo_client['TikGuard']
    mongo_collection = mongo_db['tiktok_analysis']
    logger.info("✅ MongoDB Atlas connection initialized successfully")
    return True

# ===================== MAIN CONSUMER FUNCTION =====================
def consume_and_process_atlas(max_messages=10, max_seconds=60):
    global mongo_collection, mongo_client
    logger.info("=" * 60)
    logger.info("🚀 TIKTOK KAFKA CONSUMER (MONGODB ATLAS) STARTING")
    logger.info("=" * 60)
    logger.info(f"⏰ Start time: {datetime.now()}")
    if not initialize_mongodb_atlas():
        logger.error("❌ Failed to initialize MongoDB Atlas connection")
        return
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=30000,
            session_timeout_ms=45000,
            heartbeat_interval_ms=15000
        )
        logger.info(f"🔌 Connected to Kafka topic: {KAFKA_TOPIC}")
        logger.info(f"🗄️ Using MongoDB Atlas for storage")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        logger.error("💡 Hãy kiểm tra Kafka server có đang chạy không")
        return

    processed_count = 0
    error_count = 0
    start_time = time.time()
    try:
        for i, message in enumerate(consumer):
            if (max_messages and i >= max_messages) or (max_seconds and (time.time() - start_time) > max_seconds):
                logger.info("Reached batch limit, stopping consumer.")
                break
            try:
                metadata = message.value
                video_id = metadata.get('video_id', '')
                logger.info(f"📥 Nhận metadata từ Kafka: {video_id}")
                required_fields = [
                    'video_id', 'url', 'username', 'author_name', 'description', 'hashtags',
                    'playcount', 'diggcount', 'commentcount', 'sharecount',
                    'music_title', 'music_author', 'duration', 'width', 'height', 'created_time'
                ]
                if not all(field in metadata for field in required_fields):
                    logger.warning(f"⚠️ Metadata thiếu trường cần thiết, bỏ qua: {video_id}")
                    error_count += 1
                    continue
                video_path = download_tiktok_content(video_id)
                if not video_path:
                    logger.error(f"❌ Không tải được video/slides cho {video_id}")
                    error_count += 1
                    continue
                processed_video_path = preprocess_video(video_path)
                if not processed_video_path:
                    logger.error(f"❌ Tiền xử lý thất bại cho {video_id}")
                    cleanup_files(video_id)
                    error_count += 1
                    continue
                video_record = {'video_path': processed_video_path}
                text_result = text_processor.process_video_safe(video_record)
                combined_text = text_result['combined_text']
                audio_text = text_result['audio_text']
                ocr_text = text_result['ocr_text']
                classification = classify_video_multimodal(processed_video_path, combined_text)
                logger.info(f"🔮 Prediction for video {video_id}: {classification}")
                metadata['ocr_text'] = ocr_text
                metadata['audio_text'] = audio_text
                metadata['combined_text'] = combined_text
                saved = save_to_mongo_atlas(metadata, processed_video_path, classification)
                if saved:
                    cleanup_files(video_id)
                    processed_count += 1
                    log_mongo_atlas_count()
                    elapsed_time = time.time() - start_time
                    avg_time = elapsed_time / processed_count if processed_count > 0 else 0
                    logger.info(f"📈 Progress: {processed_count} processed, {error_count} errors, Avg: {avg_time:.2f}s/video")
                else:
                    logger.warning(f"Không xóa file vì lưu MongoDB Atlas thất bại cho video {video_id}")
                    error_count += 1
            except Exception as e:
                logger.error(f"❌ Lỗi khi xử lý message: {e}")
                logger.error(traceback.format_exc())
                error_count += 1
                try:
                    mongo_client.admin.command('ping')
                except Exception as mongo_error:
                    logger.error(f"❌ MongoDB Atlas connection lost: {mongo_error}")
                    logger.error("🔄 Thử kết nối lại MongoDB Atlas...")
                    try:
                        if initialize_mongodb_atlas():
                            logger.info("✅ Reconnected to MongoDB Atlas successfully")
                        else:
                            logger.error("❌ Failed to reconnect to MongoDB Atlas")
                            break
                    except Exception as reconnect_error:
                        logger.error(f"❌ Failed to reconnect to MongoDB Atlas: {reconnect_error}")
                        break
    except KeyboardInterrupt:
        logger.info("⚠️ Dừng bởi người dùng (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Lỗi nghiêm trọng trong consumer loop: {e}")
        logger.error(traceback.format_exc())
    finally:
        try:
            consumer.close()
            logger.info("🔌 Kafka consumer closed")
        except:
            pass
        total_time = time.time() - start_time
        logger.info("🏁 Consumer kết thúc")
        logger.info(f"📊 Final Summary:")
        logger.info(f"   - Processed: {processed_count} videos")
        logger.info(f"   - Errors: {error_count}")
        logger.info(f"   - Total time: {total_time:.2f}s")
        logger.info(f"   - Avg time per video: {total_time/max(processed_count, 1):.2f}s")
        logger.info(f"   - Success rate: {processed_count/max(processed_count + error_count, 1)*100:.1f}%")
        try:
            remaining_files = [f for f in os.listdir('.') if f.startswith('downloaded_videos')]
            for f in remaining_files:
                try:
                    os.remove(f)
                    logger.info(f"🗑️ Cleanup: Đã xóa file {f}")
                except:
                    pass
        except:
            pass

# Khởi tạo MongoDB Atlas connection khi import script (cho Jupyter notebook)
try:
    initialize_mongodb_atlas()
except Exception as e:
    logger.warning(f"Không thể khởi tạo MongoDB Atlas connection: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_messages", type=int, default=10, help="Số lượng message tối đa mỗi batch")
    parser.add_argument("--max_seconds", type=int, default=60, help="Thời gian tối đa mỗi batch (giây)")
    args = parser.parse_args()
    try:
        consume_and_process_atlas(max_messages=args.max_messages, max_seconds=args.max_seconds)
    except KeyboardInterrupt:
        logger.info("⚠️ Dừng bởi người dùng (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Lỗi nghiêm trọng: {e}")
        logger.error(traceback.format_exc()) 