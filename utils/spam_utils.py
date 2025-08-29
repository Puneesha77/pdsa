import re

class SpamDetector:
    """
    Enhanced Spam Detector for chat messages.
    Detects spam using keywords, normalization, repetition, and URL patterns.
    """

    def __init__(self):
        # Common spam keywords/phrases
        self.spam_keywords = [
            "buy now", "free money", "visit this site",
            "click here", "subscribe", "lottery",
            "win cash", "make money fast", "100% free",
            "limited offer", "act now", "double your",
            "work from home"
        ]

        # Regex patterns
        self.repeated_char_pattern = re.compile(r"(.)\1{4,}")  # e.g. "heyyyyy"
        self.url_pattern = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)
        self.repeated_word_pattern = re.compile(r"\b(\w+)\s+\1\s+\1", re.IGNORECASE)  # word repeated 3+ times

    def normalize(self, text: str) -> str:
        """Remove punctuation/symbols and lowercase the text."""
        return re.sub(r"[^a-z0-9\s]", "", text.lower())

    def is_spam(self, text: str) -> bool:
        """
        Check if message is spam.
        Args:
            text: message string
        Returns:
            bool: True if spam detected
        """
        if not text:
            return False

        text_lower = text.lower()
        text_clean = self.normalize(text)

        # 1. Keyword-based detection
        for keyword in self.spam_keywords:
            if keyword in text_clean:
                return True

        # 2. Repeated character spam (e.g., "heyyyyy")
        if self.repeated_char_pattern.search(text_lower):
            return True

        # 3. Repeated word spam (e.g., "free free free")
        if self.repeated_word_pattern.search(text_lower):
            return True

        # 4. URL / suspicious links
        if self.url_pattern.search(text):
            return True

        # 5. Excessive message length
        if len(text) > 300:
            return True

        return False
