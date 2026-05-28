C_Q1 = "Q1"
C_Q2 = "Q2"
C_Q3 = "Q3"

C_SGM = "SGM"
C_SGL = "SGL"
C_SGD = "SGD"

C_Q5 = "Q5"

C_USD = "USD"
C_DATE = "DATE"

# Edge role tags prepended to Mapper→Linker payloads.
# For a transaction (from=X, to=Y) the mapper sends it twice:
#   as EDGE_A_TO_M: Y is the candidate intermediate M, X is the source A
#   as EDGE_M_TO_B: X is the candidate intermediate M, Y is the destination B
EDGE_A_TO_M = 1
EDGE_M_TO_B = 2
