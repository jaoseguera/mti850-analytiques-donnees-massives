# Cheat Sheet: Math & Python Review

Reference guide covering vector and matrix linear algebra, NumPy array manipulations, PySpark DenseVector, Python lambda expressions, and automated test validation.

---

## 1. Linear Algebra Fundamentals

### Vector Operations
Given scalar $a \in \mathbb{R}$ and vectors $\mathbf{u}, \mathbf{v} \in \mathbb{R}^n$:

- **Scalar Multiplication**:
  $$a \mathbf{v} = [a v_1, a v_2, \dots, a v_n]^T$$
  Multiplies every element of the vector by scalar $a$.

- **Element-wise Multiplication (Hadamard Product)**:
  $$\mathbf{u} \odot \mathbf{v} = [u_1 v_1, u_2 v_2, \dots, u_n v_n]^T$$
  Vectors must share identical dimensions.

- **Dot Product (Inner Product)**:
  $$\mathbf{u} \cdot \mathbf{v} = \sum_{i=1}^n u_i v_i = u_1 v_1 + u_2 v_2 + \dots + u_n v_n$$
  Produces a single scalar. If $\mathbf{u} \cdot \mathbf{v} = 0$, the vectors are orthogonal.

### Matrix Operations
Given matrix $A \in \mathbb{R}^{m \times k}$ and matrix $B \in \mathbb{R}^{k \times p}$:

- **Matrix Multiplication**:
  $$C = A B \quad \text{where} \quad C_{ij} = \sum_{r=1}^k A_{ir} B_{rj}$$
  Number of columns in $A$ must equal number of rows in $B$. Resulting matrix $C$ has dimensions $m \times p$.

- **Transpose**:
  $$(A^T)_{ij} = A_{ji}$$

- **Matrix Inverse**:
  $$A A^{-1} = A^{-1} A = I$$
  For a $2 \times 2$ matrix $A = \begin{bmatrix} a & b \\ c & d \end{bmatrix}$:
  $$A^{-1} = \frac{1}{ad - bc} \begin{bmatrix} d & -b \\ -c & a \end{bmatrix} \quad (\text{if } ad - bc \neq 0)$$

---

## 2. NumPy Reference

### Array Creation & Arithmetic
```python
import numpy as np

# Creating 1D arrays and 2D matrices
v1 = np.array([1, 2, 3])
v2 = np.array([4, 5, 6])

# Scalar multiplication
v_scaled = 3 * v1  # array([3, 6, 9])

# Element-wise operations
v_mult = v1 * v2   # array([4, 10, 18])

# Dot product (scalar output)
dot_val = np.dot(v1, v2)  # 32
# Alternatively:
dot_val2 = v1.dot(v2)
```

### Matrices, Transposition, and Inverses
```python
A = np.matrix([[1, 2, 3], [4, 5, 6]])  # shape (2, 3)

# Transpose
A_t = A.T  # shape (3, 2)

# Matrix product
AAt = A * A_t  # shape (2, 2)

# Matrix inversion
AAt_inv = AAt.I  # Or np.linalg.inv(AAt)

# Numerical comparison
np.allclose(AAt * AAt_inv, np.eye(2))  # True
```

### Slicing & Array Combination
```python
arr = np.array([0, 1, 2, 3, 4, 5])

# Slicing: arr[start:stop:step]
arr[2:5]     # array([2, 3, 4])
arr[-3:]     # array([3, 4, 5])
arr[::2]     # array([0, 2, 4])

# Combining arrays
zeros = np.zeros(8)
ones = np.ones(8)

# Horizontal stack (1D concatenation)
row = np.hstack([zeros, ones])  # shape (16,)

# Vertical stack (2D matrix rows)
grid = np.vstack([zeros, ones])  # shape (2, 8)
```

---

## 3. PySpark Linear Algebra (DenseVector)

PySpark MLlib provides `DenseVector` for scalable machine learning:

```python
from pyspark.mllib.linalg import DenseVector
import numpy as np

# Instantiate DenseVector
dv1 = DenseVector([3.0, 4.0, 5.0])
dv2 = DenseVector(np.array([-4.0, 3.0, 0.0]))

# Dot product between DenseVectors
dot_res = dv1.dot(dv2)  # -12.0 + 12.0 + 0.0 = 0.0 (orthogonal)

# Access underlying values
raw_values = dv1.values  # NumPy ndarray array([3., 4., 5.])
```

---

## 4. Python Functional Programming & Lambdas

### Lambda Syntax
```python
# Anonymous inline function: lambda arguments: expression
multiply_by_ten = lambda x: x * 10
print(multiply_by_ten(5))  # 50

# Multiple parameters
add = lambda x, y: x + y
swap = lambda pair: (pair[1], pair[0])

# Conditionals inside lambda (ternary operator)
categorize = lambda x: "positive" if x > 0 else ("negative" if x < 0 else "zero")
```

### Standard Functional Primitives
```python
from functools import reduce

data = [1, 2, 3, 4, 5]

# map(func, iterable): transforms each element
squared = list(map(lambda x: x ** 2, data))  # [1, 4, 9, 16, 25]

# filter(predicate, iterable): keeps elements where predicate is True
evens = list(filter(lambda x: x % 2 == 0, data))  # [2, 4]

# reduce(func, iterable): accumulates elements sequentially
total_sum = reduce(lambda acc, x: acc + x, data)  # 15
```

### Function Composition
```python
# Chaining functions f(g(x))
add_one = lambda x: x + 1
double = lambda x: x * 2

composed = lambda x: double(add_one(x))
print(composed(3))  # (3 + 1) * 2 = 8
```

---

## 5. Course Testing Framework (`testmti850.py`)

Usage inside review notebooks:

```python
import sys
sys.path.append('..')
import testmti850

# Verify exact match
testmti850.Test.assertEquals(result, expected_value, "Message on failure")

# Verify boolean condition
testmti850.Test.assertTrue(condition, "Message on failure")

# Verify hashed value (SHA-1 hash comparison)
testmti850.Test.assertEqualsHashed(
    actual_var,
    'e460f5b87531a2b60e0f55c31b2e49914f779981',
    "incorrect value for actual_var"
)
```
