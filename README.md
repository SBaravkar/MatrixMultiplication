# Matrix Multiplication
- Code performs multiplication of 2 matrices using MapReduce function.

### Map Function
- Map key for 1st matrix is the ('row_index', 'generating values for k via loop')
- Map value for 1st matrix is the('Matrix_Name', 'col_index', 'value at that position')
- Map key for 2nd matrix is the ('generating values for i via loop', 'col_index')
- Map value for 2nd matrix is the('Matrix_Name', 'row_index', 'value at that position')

### Reduce Function
- It multiplies the values at at each postion and then sums them.