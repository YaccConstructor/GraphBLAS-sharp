namespace CSRMultiplication

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions

module SparseMatrixMultiplication = 
    let multiplySpMV 
        (provider: ComputeProvider) 
        (commandQueue: CommandQueue) 
        (vector: float[]) 
        (matrix: CSRMatrix.CSRMatrix) =  

        let csrMatrixRowCount = matrix |> CSRMatrix.rowCount
        let csrMatrixColumnCount = matrix |> CSRMatrix.columnCount
        let vectorLength = vector.Length
        if csrMatrixColumnCount <> vectorLength then  failwith "fail"

        let resultVector = Array.zeroCreate<float> csrMatrixRowCount
        let command = 
            <@
                fun (ndRange: _1D)
                    (resultBuffer: float[]) 
                    (csrValuesBuffer: float[]) 
                    (csrColumnsBuffer: int[]) 
                    (csrRowPointersBuffer: int[]) 
                    (vectorBuffer: float[]) ->

                    let i = ndRange.GlobalID0
                    for k in csrRowPointersBuffer.[i] .. csrRowPointersBuffer.[i + 1] - 1 do
                        resultBuffer.[i] <- resultBuffer.[i] + 
                            csrValuesBuffer.[k] * vectorBuffer.[csrColumnsBuffer.[k]]       
            @>

        let (kernel, kernelPrepare, kernelRun) = provider.Compile command 
        let ndRange = _1D(csrMatrixRowCount)
        kernelPrepare 
            ndRange 
            resultVector 
            matrix.GetValues 
            matrix.GetColumns 
            matrix.GetRowPointers 
            vector
        commandQueue.Add(kernelRun()).Finish() |> ignore
        commandQueue.Add(resultVector.ToHost provider).Finish() |> ignore

        resultVector

    let multiplySpMM
        (provider: ComputeProvider) 
        (commandQueue: CommandQueue) 
        (denseMatrix: float[,]) 
        (csrMatrix: CSRMatrix.CSRMatrix) = 

        let csrMatrixRowCount = csrMatrix |> CSRMatrix.rowCount
        let csrMatrixColumnCount = csrMatrix |> CSRMatrix.columnCount
        let denseMatrixRowCount = denseMatrix |> Array2D.length1
        let denseMatrixColumnCount = denseMatrix |> Array2D.length2
        if csrMatrixColumnCount <> denseMatrixRowCount then  failwith "fail"

        let resultMatrix = Array.zeroCreate<float> (csrMatrixRowCount * denseMatrixColumnCount)
        let command = 
            <@
                fun (ndRange: _2D)
                    (resultBuffer: float[]) 
                    (csrValuesBuffer: float[]) 
                    (csrColumnsBuffer: int[]) 
                    (csrRowPointersBuffer: int[]) 
                    (denseMatrixBuffer: float[]) ->

                    let i = ndRange.GlobalID0
                    let j = ndRange.GlobalID1
                    for k in csrRowPointersBuffer.[i] .. csrRowPointersBuffer.[i + 1] - 1 do
                        resultBuffer.[i * denseMatrixColumnCount + j] <- 
                            resultBuffer.[i * denseMatrixColumnCount + j] + 
                            csrValuesBuffer.[k] * denseMatrixBuffer.[csrColumnsBuffer.[k] * denseMatrixColumnCount + j]       
            @>

        let (kernel, kernelPrepare, kernelRun) = provider.Compile command 
        let ndRange = _2D(csrMatrixRowCount, denseMatrixColumnCount)
        kernelPrepare 
            ndRange 
            resultMatrix 
            csrMatrix.GetValues 
            csrMatrix.GetColumns 
            csrMatrix.GetRowPointers 
            (denseMatrix |> Seq.cast<float> |> Seq.toArray)
        commandQueue.Add(kernelRun()).Finish() |> ignore
        commandQueue.Add(resultMatrix.ToHost provider).Finish() |> ignore

        (fun i j -> resultMatrix.[i * denseMatrixColumnCount + j])
        |> Array2D.init csrMatrixRowCount denseMatrixColumnCount
        
    let multiplySpMSpM 
        (provider: ComputeProvider) 
        (commandQueue: CommandQueue) 
        (cscMatrix: CSCMatrix.CSCMatrix) 
        (csrMatrix: CSRMatrix.CSRMatrix) = 

        let csrMatrixRowCount = csrMatrix |> CSRMatrix.rowCount
        let csrMatrixColumnCount = csrMatrix |> CSRMatrix.columnCount
        let cscMatrixRowCount = cscMatrix |> CSCMatrix.rowCount
        let cscMatrixColumnCount = cscMatrix |> CSCMatrix.columnCount
        if csrMatrixColumnCount <> cscMatrixRowCount then  failwith "fail"

        let resultMatrix = Array.zeroCreate<float> (csrMatrixRowCount * cscMatrixColumnCount)
        let command = 
            <@
                fun (ndRange: _2D)
                    (resultBuffer: float[]) 
                    (csrValuesBuffer: float[]) 
                    (csrColumnsBuffer: int[]) 
                    (csrRowPointersBuffer: int[]) 
                    (cscValuesBuffer: float[]) 
                    (cscRowsBuffer: int[]) 
                    (cscColumnPointersBuffer: int[])  ->

                    let i = ndRange.GlobalID0
                    let j = ndRange.GlobalID1
                    let mutable iPointer = csrRowPointersBuffer.[i]
                    let mutable jPointer = cscColumnPointersBuffer.[j]
                    while (iPointer < csrRowPointersBuffer.[i + 1] && jPointer < cscColumnPointersBuffer.[j + 1]) do
                        if csrColumnsBuffer.[iPointer] < cscRowsBuffer.[jPointer] then iPointer <- iPointer + 1
                        elif csrColumnsBuffer.[iPointer] > cscRowsBuffer.[jPointer] then jPointer <- jPointer + 1
                        else 
                            resultBuffer.[i * cscMatrixColumnCount + j] <- 
                                resultBuffer.[i * cscMatrixColumnCount + j] + 
                                csrValuesBuffer.[iPointer] * cscValuesBuffer.[jPointer]
                            iPointer <- iPointer + 1
                            jPointer <- jPointer + 1    
            @>

        let (kernel, kernelPrepare, kernelRun) = provider.Compile command 
        let ndRange = _2D(csrMatrixRowCount, cscMatrixColumnCount)
        kernelPrepare 
            ndRange 
            resultMatrix 
            csrMatrix.GetValues 
            csrMatrix.GetColumns 
            csrMatrix.GetRowPointers 
            cscMatrix.GetValues 
            cscMatrix.GetRows 
            cscMatrix.GetColumnPointers
        commandQueue.Add(kernelRun()).Finish() |> ignore
        commandQueue.Add(resultMatrix.ToHost provider).Finish() |> ignore

        (fun i j -> resultMatrix.[i * cscMatrixColumnCount + j])
        |> Array2D.init csrMatrixRowCount cscMatrixColumnCount

    let multiplySpMSpM2 
        (provider: ComputeProvider) 
        (commandQueue: CommandQueue) 
        (csrMatrixRight: CSRMatrix.CSRMatrix) 
        (csrMatrixLeft: CSRMatrix.CSRMatrix) = 

        let leftMatrixRowCount = csrMatrixLeft |> CSRMatrix.rowCount
        let leftMatrixColumnCount = csrMatrixLeft |> CSRMatrix.columnCount
        let rightMatrixRowCount = csrMatrixRight |> CSRMatrix.rowCount
        let rightMatrixColumnCount = csrMatrixRight |> CSRMatrix.columnCount
        if leftMatrixColumnCount <> rightMatrixRowCount then  failwith "fail"

        let resultMatrix = Array.zeroCreate<float> (leftMatrixRowCount * rightMatrixColumnCount)
        let command = 
            <@
                fun (ndRange: _2D)
                    (resultBuffer: float[]) 
                    (leftCsrValuesBuffer: float[]) 
                    (leftCsrColumnsBuffer: int[]) 
                    (leftCsrRowPointersBuffer: int[]) 
                    (rightCsrValuesBuffer: float[]) 
                    (rightCsrColumnsBuffer: int[]) 
                    (rightCsrRowPointersBuffer: int[])  ->

                    let i = ndRange.GlobalID0
                    for k in rightCsrRowPointersBuffer.[i] .. rightCsrRowPointersBuffer.[i + 1] - 1 do
                        let j = ndRange.GlobalID1
                        let mutable pointer = leftCsrRowPointersBuffer.[j]
                        while (pointer < leftCsrRowPointersBuffer.[j + 1] && leftCsrColumnsBuffer.[pointer] <= i) do
                            if leftCsrColumnsBuffer.[pointer] = i then 
                                resultBuffer.[j * rightMatrixColumnCount + rightCsrColumnsBuffer.[k]] <- 
                                    resultBuffer.[j * rightMatrixColumnCount + rightCsrColumnsBuffer.[k]] + 
                                    rightCsrValuesBuffer.[k] * leftCsrValuesBuffer.[pointer]
                            pointer <- pointer + 1
            @>

        let (kernel, kernelPrepare, kernelRun) = provider.Compile command 
        let ndRange = _2D(rightMatrixRowCount, leftMatrixRowCount)
        kernelPrepare 
            ndRange 
            resultMatrix 
            csrMatrixLeft.GetValues 
            csrMatrixLeft.GetColumns 
            csrMatrixLeft.GetRowPointers 
            csrMatrixRight.GetValues 
            csrMatrixRight.GetColumns
            csrMatrixRight.GetRowPointers
        commandQueue.Add(kernelRun()).Finish() |> ignore
        commandQueue.Add(resultMatrix.ToHost provider).Finish() |> ignore

        (fun i j -> resultMatrix.[i * rightMatrixColumnCount + j])
        |> Array2D.init leftMatrixRowCount rightMatrixColumnCount