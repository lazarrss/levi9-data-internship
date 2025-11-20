def is_palindrome(s: str) -> bool:
    if not isinstance(s, str):
        raise TypeError("Input must be a string.")
    cleaned = s.replace(" ", "").lower()
    return cleaned == cleaned[::-1]

import unittest

class TestPalindrome(unittest.TestCase):
    def test_valid_palindromes(self):
        valid_cases = [ "madam", "racecar", "a man a plan a canal panama" ]
        for text in valid_cases:
            with self.subTest(text=text):
                self.assertTrue(is_palindrome(text))

    def test_invalid_palindromes(self):
        invalid_cases = [ "chatgpt", "ai", "python" ]
        for text in invalid_cases:
            with self.subTest(text=text):
                self.assertFalse(is_palindrome(text))

    def test_empty_string(self):
        self.assertTrue(is_palindrome(""))

    def test_single_character(self):
        self.assertTrue(is_palindrome("a"))

    # error handling
    def test_invalid_input_types(self):
        invalid_inputs = [123, None, True, ["a"], {"a": 1}]

        for value in invalid_inputs:
            with self.subTest(value=value):
                with self.assertRaises(TypeError):
                    is_palindrome(value)

if __name__ == "__main__":
    unittest.main()