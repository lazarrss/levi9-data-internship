def factorial(n):
    if n < 0:
        raise ValueError("Factorial is not defined for negative numbers.")
    elif n == 0:
        return 1
    else:
        result = 1
        for i in range(1, n + 1):
            result *= i
        return result

import unittest

class TestFactorial(unittest.TestCase):
    def test_factorial_zero(self):
        self.assertEqual(factorial(0), 1)
    def test_factorial_one(self):
        self.assertEqual(factorial(1), 1)
    def test_factorial_five(self):
        self.assertEqual(factorial(5), 120)

if __name__ == "__main__":
    unittest.main()