import re
import html
import pandas as pd
import mlflow.pyfunc


class VaderRobertaModel(mlflow.pyfunc.PythonModel):
    """
    Custom MLflow pyfunc model for VADER + RoBERTa sentiment inference.

    Input:
        pandas DataFrame with a column: text

    Output:
        pandas DataFrame with:
        - prediction
        - class_id
        - confidence
        - vader_compound
    """

    def load_context(self, context):
        import nltk
        import torch
        from nltk.sentiment import SentimentIntensityAnalyzer
        from transformers import AutoTokenizer, AutoModelForSequenceClassification

        self.torch = torch

        model_name = context.model_config.get(
            "roberta_model",
            "cardiffnlp/twitter-roberta-base-sentiment",
        )

        nltk.download("vader_lexicon", quiet=True)

        self.vader = SentimentIntensityAnalyzer()
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.eval()

        self.id_to_label = {
            0: "negative",
            1: "neutral",
            2: "positive",
        }

    def clean_text(self, text: str) -> str:
        text = html.unescape(str(text))
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"http\S+|www\S+", " ", text)
        text = re.sub(r"[^a-zA-Z\s]", " ", text)
        text = text.lower()
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def predict(self, context, model_input):
        import numpy as np
        from scipy.special import softmax

        if isinstance(model_input, pd.DataFrame):
            texts = model_input["text"].tolist()
        else:
            texts = list(model_input)

        results = []

        for text in texts:
            cleaned = self.clean_text(text)

            encoded = self.tokenizer(
                cleaned,
                return_tensors="pt",
                truncation=True,
                max_length=512,
            )

            with self.torch.no_grad():
                output = self.model(**encoded)

            probabilities = softmax(output.logits[0].numpy())
            class_id = int(np.argmax(probabilities))

            vader_score = self.vader.polarity_scores(text)["compound"]

            results.append(
                {
                    "text": text,
                    "cleaned_text": cleaned,
                    "prediction": self.id_to_label[class_id],
                    "class_id": class_id,
                    "confidence": float(probabilities[class_id]),
                    "vader_compound": float(vader_score),
                }
            )

        return pd.DataFrame(results)