class LogColor:
    """
    Logger의 색상 변수들 모은 클래스
    """
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"

    @classmethod
    def success(cls, text):
        return f"{cls.GREEN}[SUCCESS] {text}{cls.RESET}"

    @classmethod
    def warn(cls, text):
        return f"{cls.YELLOW}[WARN] {text}{cls.RESET}"

    @classmethod
    def info(cls, text):
        return f"{cls.BLUE}[INFO] {text}{cls.RESET}"