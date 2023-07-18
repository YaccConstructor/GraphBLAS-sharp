namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions

module Operations =
    module Vector =
        /// <summary>
        /// Builds a new vector whose values are the results of applying the given function
        /// to the corresponding pairs of values from the two vectors.
        /// </summary>
        /// <param name="op">
        /// A function to transform pairs of values from the input vectors.
        /// Operands and result types should be optional to distinguish explicit and implicit zeroes.
        /// </param>
        /// <remarks>
        /// Formats of the given vectors should match, otherwise an exception will be thrown.
        /// </remarks>
        /// <param name="clContext">OpenCL context.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        let map2 (op: Expr<'a option -> 'b option -> 'c option>) (clContext: ClContext) workGroupSize =
            let map2Dense =
                Dense.Vector.map2 op clContext workGroupSize

            let map2Sparse =
                Sparse.Vector.map2 op clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
                match leftVector, rightVector with
                | ClVector.Dense left, ClVector.Dense right ->
                    ClVector.Dense
                    <| map2Dense processor allocationMode left right
                | ClVector.Sparse left, ClVector.Sparse right ->
                    ClVector.Sparse
                    <| map2Sparse processor allocationMode left right
                | _ -> failwith "Vector formats are not matching."

        /// <summary>
        /// Builds a new vector whose values are the results of applying the given function
        /// to the corresponding pairs of values from the two vectors.
        /// </summary>
        /// <param name="op">
        /// A function to transform pairs of values from the input vectors.
        /// Operation assumption: one of the operands should always be non-zero.
        /// </param>
        /// <remarks>
        /// Formats of the given vectors should match, otherwise an exception will be thrown.
        /// </remarks>
        /// <param name="clContext">OpenCL context.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        let map2AtLeastOne (op: Expr<AtLeastOne<'a, 'b> -> 'c option>) (clContext: ClContext) workGroupSize =
            let map2Sparse =
                Sparse.Vector.map2AtLeastOne op clContext workGroupSize

            let map2Dense =
                Dense.Vector.map2AtLeastOne op clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode (leftVector: ClVector<'a>) (rightVector: ClVector<'b>) ->
                match leftVector, rightVector with
                | ClVector.Sparse left, ClVector.Sparse right ->
                    ClVector.Sparse
                    <| map2Sparse processor allocationMode left right
                | ClVector.Dense left, ClVector.Dense right ->
                    ClVector.Dense
                    <| map2Dense processor allocationMode left right
                | _ -> failwith "Vector formats are not matching."

    module Matrix =
        /// <summary>
        /// Builds a new matrix whose values are the results of applying the given function
        /// to the corresponding pairs of values from the two matrices.
        /// </summary>
        /// <param name="op">
        /// A function to transform pairs of values from the input matrices.
        /// Operands and result types should be optional to distinguish explicit and implicit zeroes
        /// </param>
        /// <remarks>
        /// Formats of the given matrices should match, otherwise an exception will be thrown.
        /// </remarks>
        /// <param name="clContext">OpenCL context.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        let map2 (op: Expr<'a option -> 'b option -> 'c option>) (clContext: ClContext) workGroupSize =
            let map2COO =
                COO.Matrix.map2 op clContext workGroupSize

            let map2CSR =
                CSR.Matrix.map2 op clContext workGroupSize

            let transposeCOO =
                COO.Matrix.transposeInPlace clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode matrix1 matrix2 ->
                match matrix1, matrix2 with
                | ClMatrix.COO m1, ClMatrix.COO m2 ->
                    map2COO processor allocationMode m1 m2
                    |> ClMatrix.COO
                | ClMatrix.CSR m1, ClMatrix.CSR m2 ->
                    map2CSR processor allocationMode m1 m2
                    |> ClMatrix.COO
                | ClMatrix.CSC m1, ClMatrix.CSC m2 ->
                    (map2CSR processor allocationMode m1.ToCSR m2.ToCSR)
                    |> transposeCOO processor
                    |> ClMatrix.COO
                | _ -> failwith "Matrix formats are not matching"

        /// <summary>
        /// Builds a new matrix whose values are the results of applying the given function
        /// to the corresponding pairs of values from the two matrices.
        /// </summary>
        /// <param name="op">
        /// A function to transform pairs of values from the input matrices.
        /// Operation assumption: one of the operands should always be non-zero.
        /// </param>
        /// <remarks>
        /// Formats of the given matrices should match, otherwise an exception will be thrown.
        /// </remarks>
        /// <param name="clContext">OpenCL context.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        let map2AtLeastOne (op: Expr<AtLeastOne<'a, 'b> -> 'c option>) (clContext: ClContext) workGroupSize =
            let COOMap2 =
                COO.Matrix.map2AtLeastOne clContext op workGroupSize

            let CSRMap2 =
                CSR.Matrix.map2AtLeastOne clContext op workGroupSize

            let COOTranspose =
                COO.Matrix.transposeInPlace clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode matrix1 matrix2 ->
                match matrix1, matrix2 with
                | ClMatrix.COO m1, ClMatrix.COO m2 ->
                    COOMap2 processor allocationMode m1 m2
                    |> ClMatrix.COO
                | ClMatrix.CSR m1, ClMatrix.CSR m2 ->
                    CSRMap2 processor allocationMode m1 m2
                    |> ClMatrix.COO
                | ClMatrix.CSC m1, ClMatrix.CSC m2 ->
                    (CSRMap2 processor allocationMode m1.ToCSR m2.ToCSR)
                    |> COOTranspose processor
                    |> ClMatrix.COO
                | _ -> failwith "Matrix formats are not matching"

    /// <summary>
    /// Matrix-vector multiplication.
    /// </summary>
    /// <param name="add">Type of binary function to reduce entries.</param>
    /// <param name="mul">Type of binary function to combine entries.</param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let SpMV
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let run = SpMV.run add mul clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix<'a>) (vector: ClVector<'b>) ->
            match matrix, vector with
            | ClMatrix.CSR m, ClVector.Dense v -> run queue allocationMode m v |> ClVector.Dense
            | _ -> failwith "Not implemented yet"


    /// <summary>
    /// Kronecker product for matrices.
    /// </summary>
    /// <param name="op">
    /// Element-wise operation.
    /// Operands and result types should be optional to distinguish explicit and implicit zeroes
    /// </param>
    /// <param name="clContext">OpenCL context.</param>
    /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let kronecker (op: Expr<'a option -> 'b option -> 'c option>) (clContext: ClContext) workGroupSize =
        let run =
            CSR.Matrix.kronecker clContext workGroupSize op

        fun (queue: MailboxProcessor<_>) allocationFlag (matrix1: ClMatrix<'a>) (matrix2: ClMatrix<'b>) ->
            match matrix1, matrix2 with
            | ClMatrix.CSR m1, ClMatrix.CSR m2 ->
                let result = run queue allocationFlag m1 m2
                Option.map ClMatrix.COO result
            | _ -> failwith "Both matrices should be in CSR format."

    /// <summary>
    /// Sparse general matrix-matrix multiplication.
    /// </summary>
    module SpGeMM =
        /// <summary>
        /// Masked matrix-matrix multiplication.
        /// </summary>
        /// <param name="opAdd">Type of binary function to reduce entries.</param>
        /// <param name="opMul">Type of binary function to combine entries.</param>
        /// <param name="clContext">OpenCL context.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        let masked
            (opAdd: Expr<'c -> 'c -> 'c option>)
            (opMul: Expr<'a -> 'b -> 'c option>)
            (clContext: ClContext)
            workGroupSize
            =

            let runCSRnCSC =
                SpGeMM.Masked.run opAdd opMul clContext workGroupSize

            fun (queue: MailboxProcessor<_>) (matrix1: ClMatrix<'a>) (matrix2: ClMatrix<'b>) (mask: ClMatrix<_>) ->
                match matrix1, matrix2, mask with
                | ClMatrix.CSR m1, ClMatrix.CSC m2, ClMatrix.COO mask -> runCSRnCSC queue m1 m2 mask |> ClMatrix.COO
                | _ -> failwith "Matrix formats are not matching"

        /// <summary>
        /// Generalized matrix-matrix multiplication.
        /// </summary>
        /// <param name="opAdd">Type of binary function to reduce entries.</param>
        /// <param name="opMul">Type of binary function to combine entries.</param>
        /// <param name="clContext">OpenCL context.</param>
        /// <param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
        let expand
            (opAdd: Expr<'c -> 'c -> 'c option>)
            (opMul: Expr<'a -> 'b -> 'c option>)
            (clContext: ClContext)
            workGroupSize
            =

            let run =
                SpGeMM.Expand.run opAdd opMul clContext workGroupSize

            fun (processor: MailboxProcessor<_>) allocationMode (leftMatrix: ClMatrix<'a>) (rightMatrix: ClMatrix<'b>) ->
                match leftMatrix, rightMatrix with
                | ClMatrix.CSR leftMatrix, ClMatrix.CSR rightMatrix ->
                    let allocCapacity =
                        List.max [ sizeof<'a>
                                   sizeof<'c>
                                   sizeof<'b> ]
                        * 1<Byte>

                    let resultCapacity =
                        (clContext.MaxMemAllocSize / allocCapacity) / 3

                    run processor allocationMode resultCapacity leftMatrix rightMatrix
                | _ -> failwith "Matrix formats are not matching"

