namespace GraphBLAS.FSharp.Backend.CSRMatrix

// open Brahma.FSharp.OpenCL
// open GraphBLAS.FSharp
// open GraphBLAS.FSharp.Backend.Common
// open Brahma.OpenCL
// open System

// module internal rec Mxm =
//     let [<Literal>] BinsCount = 8
//     let [<Literal>] MaxBinId = 7
//     let [<Literal>] Warp = 32
//     let [<Literal>] Pwarp = 4
//     let [<Literal>] HashScale = 107

//     // TODO добавить корректную обработку нулевых матриц
//     let inline run (leftMatrix: CSRMatrix<'a>) (rightMatrix: CSRMatrix<'a>) (semiring: ISemiring<'a>) = opencl {
//         if leftMatrix.Values.Length = 0 then
//             let! resultRows = Copy.copyArray leftMatrix.RowPointers
//             let! resultColumns = Copy.copyArray leftMatrix.ColumnIndices
//             let! resultValues = Copy.copyArray leftMatrix.Values

//             return {
//                 RowPointers = resultRows
//                 ColumnIndices = resultColumns
//                 Values = resultValues
//                 RowCount = leftMatrix.RowCount
//                 ColumnCount = rightMatrix.ColumnCount
//             }

//         elif rightMatrix.Values.Length = 0 then
//             let! resultRows = Copy.copyArray rightMatrix.RowPointers
//             let! resultColumns = Copy.copyArray rightMatrix.ColumnIndices
//             let! resultValues = Copy.copyArray rightMatrix.Values

//             return {
//                 RowPointers = resultRows
//                 ColumnIndices = resultColumns
//                 Values = resultValues
//                 RowCount = leftMatrix.RowCount
//                 ColumnCount = rightMatrix.ColumnCount
//             }

//         else

//             // (1) Count the number of intermediate products of each row
//             let! numberOfIntermediateProducts = getNumberOfIntermediateProducts leftMatrix rightMatrix
//             let! _ = ToHost numberOfIntermediateProducts

//             // (2) Divide the rows into groups by the number of intermediate products
//             let (bins, globalTableOffsets, globalTableMemorySize) = divideIntoBins numberOfIntermediateProducts
//             let (flatBins, binsPointers) = flattenBins leftMatrix.RowCount bins

//             // (3) Count the number of non-zero elements of each row of output matrix for all bins
//             let! nnzEstimation = getNnzEstimation leftMatrix rightMatrix flatBins binsPointers globalTableOffsets globalTableMemorySize

//             // (4) Set row pointers of output matrix to store in CSR by scan
//             let nnz = [| 0 |]
//             // let! outputPointers = PrefixSum.runExcludeInplace nnzEstimation nnz
//             do! PrefixSum.runExcludeInplace nnzEstimation nnz
//             let! _ = ToHost nnz

//             // (5) Memory allocation of output matrix
//             let! outputPointers = Copy.copyArray nnzEstimation
//             let outputColumns = Array.zeroCreate<int> nnz.[0]
//             let outputValues = Array.zeroCreate<'a> nnz.[0]
//             let output = {
//                 RowPointers = outputPointers
//                 ColumnIndices = outputColumns
//                 Values = outputValues
//                 RowCount = leftMatrix.RowCount
//                 ColumnCount = rightMatrix.ColumnCount
//             }

//             // (6) Divide the rows into groups by the number of nnz

//             // (7) Calc the output matrix (calc values and column indices -> shrink table -> sort)
//             do! calculateResult leftMatrix rightMatrix semiring flatBins binsPointers globalTableOffsets globalTableMemorySize output

//             return output
//     }

//     let getNumberOfIntermediateProducts<'a> (leftMatrix: CSRMatrix<'a>) (rightMatrix: CSRMatrix<'a>) = opencl {
//         let leftRowCount = leftMatrix.RowCount

//         let kernel =
//             <@
//                 fun (ndRange: _1D)
//                     (leftPointers: int[])
//                     (leftColumns: int[])
//                     (rightPointers: int[])
//                     (numberOfIntermediateProducts: int[]) ->

//                     let i = ndRange.GlobalID0

//                     if i < leftRowCount then
//                         let mutable acc = 0
//                         for j = leftPointers.[i] to leftPointers.[i + 1] do
//                             let col = leftColumns.[j]
//                             acc <- acc + (rightPointers.[col + 1] - rightPointers.[col])

//                         numberOfIntermediateProducts.[i] <- acc
//             @>

//         let numberOfIntermediateProducts = Array.zeroCreate<int> leftRowCount

//         do! RunCommand kernel <| fun kernelPrepare ->
//             let range = _1D(Utils.getDefaultGlobalSize leftRowCount, Utils.defaultWorkGroupSize)
//             kernelPrepare
//                 range
//                 leftMatrix.RowPointers
//                 leftMatrix.ColumnIndices
//                 rightMatrix.RowPointers
//                 numberOfIntermediateProducts

//         return numberOfIntermediateProducts
//     }

//     // NOTE: NVIDIA can operate more than 256 threads per group, but AMD cannot
//     let getWorkGroupSize = function
//         | 1 -> 64
//         | 2 -> 128
//         | 0 | 3 | 4 | 5 | 6 | 7 -> 256
//         | _ -> failwith "Unknown bin id"

//     let getBinId = function
//         | x when x <= 32 -> 0
//         | x when x <= 128 -> 1
//         | x when x <= 256 -> 2
//         | x when x <= 512 -> 3
//         | x when x <= 1024 -> 4
//         | x when x <= 2048 -> 5
//         | x when x <= 4096 -> 6
//         | _ -> 7

//     let getTableSize = function
//         | 0 -> 32
//         | 1 -> 128
//         | 2 -> 256
//         | 3 -> 512
//         | 4 -> 1024
//         | 5 -> 2048
//         | 6 -> 4096
//         | _ -> failwith "Table size is only valid for 0 - 6 bin"

//     let divideIntoBins (numberOfIntermediateProducts: int[]) =
//         let rowCount = numberOfIntermediateProducts.Length
//         let bins = Array.create<int list> BinsCount []
//         let mutable globalTableOffsets = []
//         let mutable globalTableMemorySize = 0
//         let mutable preNnz = 0
//         for rowIdx = 0 to rowCount - 1 do
//             let current = numberOfIntermediateProducts.[rowIdx]
//             let bin = getBinId current
//             // TODO: need to optimize
//             bins.[bin] <- bins.[bin] @ [rowIdx]

//             preNnz <- preNnz + current
//             if bin = MaxBinId then
//                 globalTableOffsets <- globalTableOffsets @ [globalTableMemorySize]
//                 globalTableMemorySize <- globalTableMemorySize + current

//         (bins, globalTableOffsets |> Array.ofList, globalTableMemorySize)

//     let flattenBins (rowCount: int) (bins: int list []) =
//         let flatBins = Array.zeroCreate<int> rowCount
//         let binsPointers = Array.zeroCreate<int> (BinsCount + 1)
//         let mutable binOffset = 0
//         for binId = 0 to BinsCount - 1 do
//             let bin = bins.[binId]
//             binsPointers.[binId] <- binOffset
//             Array.blit (Array.ofList bin) 0 flatBins binOffset bin.Length
//             binOffset <- binOffset + bin.Length

//         binsPointers.[BinsCount] <- binOffset

//         (flatBins, binsPointers)

//     let getBinLen (i: int) (binsPointers: int[]) =
//         binsPointers.[i + 1] - binsPointers.[i]

//     let inline getNnzEstimation
//         (leftMatrix: CSRMatrix<'a>)
//         (rightMatrix: CSRMatrix<'a>)
//         (flatBins: int[])
//         (binsPointers: int[])
//         (globalTableOffsets: int[])
//         (globalTableMemorySize: int) = opencl {

//         let symbolicPW nnzEstimation = opencl {
//             let binId = 0
//             let binLength = getBinLen binId binsPointers
//             let wgSize = getWorkGroupSize binId
//             let tableSize = getTableSize binId
//             let pwarpsInWG = wgSize / Pwarp

//             let localTableSizePW = pwarpsInWG * tableSize

//             let kernel =
//                 <@
//                     fun (ndRange: _1D)
//                         (flatBins: int[]) // индексы строк d_row_perm
//                         (binOffset: int) // bin_offset
//                         (binLength: int)
//                         (leftPointers: int[])
//                         (leftColumns: int[])
//                         (rightPointers: int[])
//                         (rightColumns: int[])
//                         (nnzEstimation: int[]) -> // d_row_nz

//                         let gid = ndRange.GlobalID0

//                         // id пворпа
//                         let pwarpId = gid / Pwarp
//                         // id пворпа в рабочей группе
//                         let localPwarpId = pwarpId % pwarpsInWG
//                         // id итема в пворпе
//                         let pwarpLocalId = gid % Pwarp
//                         // индекс в массиве бинов
//                         let binId = binOffset + pwarpId
//                         let rowIdx = flatBins.[binId]

//                         // хеш-таблица в локальной памяти и индекс для конкретной строки левой матрицы
//                         let table = localArray<int> localTableSizePW
//                         let pwarpTableIdx = tableSize * localPwarpId

//                         let mutable i = pwarpLocalId
//                         while i < tableSize do
//                             table.[pwarpTableIdx + i] <- -1
//                             i <- i + Pwarp

//                         let nnzBuffer = localArray<int> wgSize
//                         let threadNnzBufferIdx = Pwarp * localPwarpId + pwarpLocalId
//                         nnzBuffer.[threadNnzBufferIdx] <- 0

//                         barrier ()

//                         if binId < binOffset + binLength then
//                             let mutable j = leftPointers.[rowIdx]
//                             while j < leftPointers.[rowIdx + 1] do
//                                 let d = leftColumns.[j + pwarpLocalId]

//                                 for k = rightPointers.[d] to rightPointers.[d + 1] - 1 do
//                                     let key = rightColumns.[k]
//                                     let mutable hash = (key * HashScale) % tableSize

//                                     let mutable _break = false
//                                     while not _break do
//                                         if table.[pwarpTableIdx + hash] = key then
//                                             _break <- true
//                                         elif table.[hash] = -1 then
//                                             let old = aCompExchR table.[pwarpTableIdx + hash] (-1) key
//                                             if old = -1 then
//                                                 nnzBuffer.[threadNnzBufferIdx] <- nnzBuffer.[threadNnzBufferIdx] + 1
//                                                 _break <- true
//                                         else
//                                             hash <- (hash + 1) % tableSize

//                                 j <- j + Pwarp

//                         barrier ()

//                         if binId < binOffset + binLength then
//                             if pwarpLocalId = 0 then
//                                 nnzEstimation.[rowIdx] <-
//                                     nnzBuffer.[threadNnzBufferIdx] +
//                                     nnzBuffer.[threadNnzBufferIdx + 1] +
//                                     nnzBuffer.[threadNnzBufferIdx + 2] +
//                                     nnzBuffer.[threadNnzBufferIdx + 3]
//                 @>

//             do! RunCommand kernel <| fun kernelPrepare ->
//                 kernelPrepare
//                 <| _1D(binLength * Pwarp |> Utils.getValidGlobalSize wgSize, wgSize)
//                 <| flatBins
//                 <| binsPointers.[binId]
//                 <| binLength
//                 <| leftMatrix.RowPointers
//                 <| leftMatrix.ColumnIndices
//                 <| rightMatrix.RowPointers
//                 <| rightMatrix.ColumnIndices
//                 <| nnzEstimation
//         }

//         // wg per row, warp per item in a
//         let symbolicWG binId nnzEstimation = opencl {
//             let binLength = getBinLen binId binsPointers
//             let wgSize = getWorkGroupSize binId
//             let warpsInWG = wgSize / Warp
//             let tableSize = getTableSize binId

//             let kernel =
//                 <@
//                     fun (ndRange: _1D)
//                         (flatBins: int[])
//                         (binOffset: int)
//                         (binLength: int)
//                         (leftPointers: int[])
//                         (leftColumns: int[])
//                         (rightPointers: int[])
//                         (rightColumns: int[])
//                         (nnzEstimation: int[]) ->

//                         let gid = ndRange.GlobalID0
//                         let lid = ndRange.LocalID0

//                         let warpLocalId = lid % Warp
//                         let warpId = lid / Warp
//                         let wgId = gid / wgSize
//                         let binId = binOffset + wgId
//                         let rowIdx = flatBins.[binId]

//                         let table = localArray<int> tableSize
//                         for i in lid .. wgSize .. tableSize - 1 do
//                             table.[i] <- -1

//                         let nnzBuffer = localArray<int> wgSize
//                         nnzBuffer.[lid] <- 0

//                         barrier ()

//                         for j in leftPointers.[rowIdx] + warpId .. warpsInWG .. leftPointers.[rowIdx + 1] - 1 do
//                             let d = leftColumns.[j]

//                             for k in rightPointers.[d] + warpLocalId .. Warp .. rightPointers.[d + 1] - 1 do
//                                 let key = rightColumns.[k]
//                                 let mutable hash = (key * HashScale) % tableSize

//                                 let mutable _break = false
//                                 while not _break do
//                                     if table.[hash] = key then
//                                         _break <- true
//                                     elif table.[hash] = -1 then
//                                         let old = aCompExchR table.[hash] (-1) key
//                                         if old = -1 then
//                                             nnzBuffer.[lid] <- nnzBuffer.[lid] + 1
//                                             _break <- true
//                                     else
//                                         hash <- (hash + 1) % tableSize

//                         barrier ()

//                         let mutable step = wgSize / 2
//                         while step > 0 do
//                             if lid < step then
//                                 nnzBuffer.[lid] <- nnzBuffer.[lid] + nnzBuffer.[lid + step]
//                             barrier ()
//                             step <- step / 2

//                         if lid = 0 then
//                             nnzEstimation.[rowIdx] <- nnzEstimation.[0]
//                 @>

//             do! RunCommand kernel <| fun kernelPrepare ->
//                 kernelPrepare
//                 <| _1D(binLength * wgSize |> Utils.getDefaultGlobalSize, wgSize)
//                 <| flatBins
//                 <| binsPointers.[binId]
//                 <| binLength
//                 <| leftMatrix.RowPointers
//                 <| leftMatrix.ColumnIndices
//                 <| rightMatrix.RowPointers
//                 <| rightMatrix.ColumnIndices
//                 <| nnzEstimation
//         }

//         // let bitonicSort (array: int[]) =
//         //     let arraySize = array.Length
//         //     let wgSize = 256

//         //     let ceilToPower2 value =
//         //         let mutable v = value
//         //         v <- v - 1
//         //         v <- v ||| (v >>> 1)
//         //         v <- v ||| (v >>> 2)
//         //         v <- v ||| (v >>> 4)
//         //         v <- v ||| (v >>> 8)
//         //         v <- v ||| (v >>> 16)
//         //         v <- v + 1
//         //         v

//         //     let kernel =
//         //         <@
//         //             fun (ndRange: _1D)
//         //                 (data: int[]) ->

//         //                 let lid = ndRange.LocalID0
//         //                 let outer = ceilToPower2 arraySize
//         //                 let threadsNeeded = outer / 2
//         //                 let segLen = 2
//         //                 while outer <> 1 do
//         //                     for i in lid .. wgSize .. threadsNeeded - 1 do
//         //                         let halfSegLen = segLen / 2
//         //                         let localLineId = i % halfSegLen
//         //                         let localTwinId = segLen - localLineId - 1
//         //                         let groupLineId = i / halfSegLen
//         //                         let lineId = segLen * groupLineId + localLineId
//         //                         let twinId = segLen * groupLineId + localTwinId

//         //                         if lineId < arraySize
//         //                         && twinId < arraySize
//         //                         && data.[lineId] > data.[twinId]
//         //                         then
//         //                             let temp = data.[lineId]
//         //                             data.[lineId] <- data.[twinId]
//         //                             data.[twinId] <- temp

//         //                     barrier ()

//         //                 // for j =
//         //         @>

//         //     ()

//         let symbolicGlobal globalTable globalTableOffsets nnzEstimation = opencl {
//             let binId = MaxBinId
//             let binLength = getBinLen binId binsPointers
//             let wgSize = getWorkGroupSize binId
//             let warpsInWG = wgSize / Warp
//             let tableSize = getTableSize binId

//             let kernel =
//                 <@
//                     fun (ndRange: _1D)
//                         (flatBins: int[])
//                         (binOffset: int)
//                         (binLength: int)
//                         (globalTable: int[])
//                         (globalTableOffsets: int[])
//                         (leftPointers: int[])
//                         (leftColumns: int[])
//                         (rightPointers: int[])
//                         (rightColumns: int[])
//                         (nnzEstimation: int[]) ->

//                         let gid = ndRange.GlobalID0
//                         let lid = ndRange.LocalID0

//                         let warpLocalId = lid % Warp
//                         let warpId = lid / Warp
//                         let wgId = gid / wgSize
//                         let binId = binOffset + wgId
//                         let rowIdx = flatBins.[binId]

//                         let localTableSize = globalTableOffsets.[wgId + 1] - globalTableOffsets.[wgId]
//                         let localTableIdx = globalTableOffsets.[wgId]
//                         for i in lid .. wgSize .. localTableSize - 1 do
//                             globalTable.[i] <- -1

//                         let nnzBuffer = localArray<int> wgSize
//                         nnzBuffer.[lid] <- 0

//                         barrier ()

//                         for j in leftPointers.[rowIdx] + warpId .. warpsInWG .. leftPointers.[rowIdx + 1] - 1 do
//                             let d = leftColumns.[j]

//                             for k in rightPointers.[d] + warpLocalId .. Warp .. rightPointers.[d + 1] do
//                                 let key = rightColumns.[k]
//                                 let mutable hash = (key * HashScale) % tableSize

//                                 let mutable _break = false
//                                 while not _break do
//                                     if globalTable.[localTableIdx + hash] = key then
//                                         _break <- true
//                                     elif globalTable.[localTableIdx + hash] = -1 then
//                                         let old = aCompExchR globalTable.[localTableIdx + hash] (-1) key
//                                         if old = -1 then
//                                             nnzBuffer.[lid] <- nnzBuffer.[lid] + 1
//                                             _break <- true
//                                     else
//                                         hash <- (hash + 1) % tableSize

//                         barrier ()

//                         let mutable step = wgSize / 2
//                         while step > 0 do
//                             if lid < step then
//                                 nnzBuffer.[lid] <- nnzBuffer.[lid] + nnzBuffer.[lid + step]
//                             barrier ()
//                             step <- step / 2


//                         // bitonic sort

//                         if lid = 0 then
//                             nnzEstimation.[rowIdx] <- nnzEstimation.[0]
//                 @>

//             do! RunCommand kernel <| fun kernelPrepare ->
//                 kernelPrepare
//                 <| _1D(binLength * wgSize |> Utils.getDefaultGlobalSize, wgSize)
//                 <| flatBins
//                 <| binsPointers.[binId]
//                 <| binLength
//                 <| globalTable
//                 <| globalTableOffsets
//                 <| leftMatrix.RowPointers
//                 <| leftMatrix.ColumnIndices
//                 <| rightMatrix.RowPointers
//                 <| rightMatrix.ColumnIndices
//                 <| nnzEstimation
//         }

//         // nnz of each row in c
//         let nnzEstimation = Array.zeroCreate<int> flatBins.Length // count of rows (M)

//         for binId = 0 to BinsCount - 1 do
//             if getBinLen binId binsPointers <> 0 then // bin_size[i]
//                 if binId = 0 then
//                     do! symbolicPW nnzEstimation
//                 else
//                     failwith "kekw"
//                 // elif binId <> MaxBinId then
//                 //     do! symbolicWG binId nnzEstimation
//                 // else
//                 //     let globalTable = Array.zeroCreate<int> globalTableMemorySize
//                 //     do! symbolicGlobal globalTable globalTableOffsets nnzEstimation

//         return nnzEstimation
//     }

//     let inline calculateResult
//         (leftMatrix: CSRMatrix<'a>)
//         (rightMatrix: CSRMatrix<'a>)
//         (semiring: ISemiring<'a>)
//         (flatBins: int[])
//         (binsPointers: int[])
//         (globalTableOffsets: int[])
//         (globalTableMemorySize: int)
//         (outputMatrix: CSRMatrix<'a>) = opencl {

//         let zero = semiring.Zero
//         let (ClosedBinaryOp plus) = semiring.Plus
//         let (ClosedBinaryOp times) = semiring.Times

//         let numericPW glnz = opencl {
//             let binId = 0
//             let binLength = getBinLen binId binsPointers
//             let wgSize = getWorkGroupSize binId
//             let tableSize = getTableSize binId
//             let pwarpsInWG = wgSize / Pwarp

//             let localTableSizePW = pwarpsInWG * tableSize

//             let kernel =
//                 <@
//                     fun (ndRange: _1D)
//                         (flatBins: int[]) // индексы строк d_row_perm
//                         (binOffset: int)
//                         (binLength: int)
//                         (glNz: int[])
//                         (leftPointers: int[])
//                         (leftColumns: int[])
//                         (leftValues: 'a[])
//                         (rightPointers: int[])
//                         (rightColumns: int[])
//                         (rightValues: 'a[])
//                         (outputPointers: int[]) // дано
//                         (outputColumns: int[])
//                         (outputValues: 'a[]) ->

//                         let gid = ndRange.GlobalID0

//                         // id пворпа rid
//                         let pwarpId = gid / Pwarp
//                         // id пворпа в рабочей группе local_rid
//                         let localPwarpId = pwarpId % pwarpsInWG
//                         // id итема в пворпе tid
//                         let pwarpLocalId = gid % Pwarp

//                         let localOffset = tableSize * localPwarpId

//                         // хеш-таблица в локальной памяти и индекс для конкретной строки левой матрицы
//                         let table = localArray<int> localTableSizePW
//                         let localValues = localArray<'a> localTableSizePW
//                         let mutable i = pwarpLocalId
//                         while i < tableSize do
//                             table.[localOffset + i] <- -1
//                             localValues.[localOffset + i] <- zero
//                             i <- i + Pwarp

//                         let localNz = localArray<int> pwarpsInWG
//                         if pwarpLocalId = 0 then localNz.[pwarpLocalId] <- 0

//                         // if pwarpId >= binLength then ()
//                         // индекс в массиве бинов
//                         let binId = binOffset + pwarpId
//                         let rowIdx = flatBins.[binId]

//                         if pwarpLocalId = 0 then glNz.[rowIdx] <- 0

//                         barrier ()

//                         if binId < binOffset + binLength then
//                             let mutable j = leftPointers.[rowIdx]
//                             while j < leftPointers.[rowIdx + 1] do
//                                 let aCol = leftColumns.[j]
//                                 let aVal = leftValues.[j]

//                                 for k = rightPointers.[aCol] to rightPointers.[aCol + 1] - 1 do
//                                     let bCol = rightColumns.[k]
//                                     let bVal = rightValues.[k]

//                                     let key = bCol
//                                     let mutable hash = (key * HashScale) % tableSize

//                                     let mutable _break = false
//                                     while not _break do
//                                         if table.[localOffset + hash] = key then
//                                             // TODO make it arbitrary (not atomic add but plus)
//                                             //localValues.[localOffset + hash] <!+ aVal * bVal
//                                             _break <- true
//                                         elif table.[localOffset + hash] = -1 then
//                                             let old = aCompExchR table.[localOffset + hash] (-1) key
//                                             if old = -1 then
//                                                 // TODO make it arbitrary (not atomic add but plus)
//                                                 //localValues.[localOffset + hash] <!+ aVal * bVal
//                                                 _break <- true
//                                         else
//                                             hash <- (hash + 1) % tableSize

//                                 j <- j + Pwarp

//                         barrier ()

//                         if binId < binOffset + binLength then
//                             let mutable i = pwarpLocalId
//                             while i < tableSize do
//                                 if table.[localOffset + i] <> -1 then
//                                     let idx = localNz.[localPwarpId] <!+> 1
//                                     table.[localOffset + idx] <- table.[localOffset + i]
//                                     localValues.[localOffset + idx] <- localValues.[localOffset + i]
//                                 i <- i + Pwarp

//                         barrier ()

//                         if binId < binOffset + binLength then
//                             let nz = localNz.[localPwarpId]
//                             let globalColumnsIdx = outputPointers.[rowIdx]
//                             let mutable i = pwarpLocalId
//                             while i < nz do
//                                 let target = table.[localOffset + i]
//                                 let mutable count = 0
//                                 for k = 0 to nz - 1 do
//                                     count <- int (uint32 (table.[localOffset + k]  - target) >>> 31)
//                                 outputColumns.[globalColumnsIdx + count] <- table.[localOffset + i]
//                                 outputValues.[globalColumnsIdx + count] <- localValues.[localOffset + i]
//                                 i <- i + Pwarp
//                 @>

//             do! RunCommand kernel <| fun kernelPrepare ->
//                 kernelPrepare
//                 <| _1D(binLength * Pwarp |> Utils.getDefaultGlobalSize, wgSize)
//                 <| flatBins
//                 <| binsPointers.[binId]
//                 <| binLength
//                 <| glnz
//                 <| leftMatrix.RowPointers
//                 <| leftMatrix.ColumnIndices
//                 <| leftMatrix.Values
//                 <| rightMatrix.RowPointers
//                 <| rightMatrix.ColumnIndices
//                 <| rightMatrix.Values
//                 <| outputMatrix.RowPointers
//                 <| outputMatrix.ColumnIndices
//                 <| outputMatrix.Values
//         }

//         let numericWG binId glnz = opencl {
//             let binLength = getBinLen binId binsPointers
//             let wgSize = getWorkGroupSize binId
//             let warpsInWG = wgSize / Warp
//             let tableSize = getTableSize binId

//             let kernel =
//                 <@
//                     fun (ndRange: _1D)
//                         (flatBins: int[]) // индексы строк d_row_perm
//                         (binOffset: int)
//                         (binLength: int)
//                         (glNz: int[])
//                         (leftPointers: int[])
//                         (leftColumns: int[])
//                         (leftValues: 'a[])
//                         (rightPointers: int[])
//                         (rightColumns: int[])
//                         (rightValues: 'a[])
//                         (outputPointers: int[]) // дано
//                         (outputColumns: int[])
//                         (outputValues: 'a[]) ->

//                         let gid = ndRange.GlobalID0
//                         let lid = ndRange.LocalID0

//                         let warpLocalId = lid % Warp // tid
//                         let warpId = lid / Warp // wid
//                         let wgId = gid / wgSize // rid

//                         // хеш-таблица в локальной памяти и индекс для конкретной строки левой матрицы
//                         let table = localArray<int> tableSize
//                         let localValues = localArray<'a> tableSize
//                         for i in lid .. wgSize .. tableSize - 1 do
//                             table.[i] <- -1
//                             localValues.[i] <- zero

//                         // if pwarpId >= binLength then ()
//                         // индекс в массиве бинов
//                         let binId = binOffset + wgId
//                         let rowIdx = flatBins.[binId]

//                         if warpLocalId = 0 then glNz.[rowIdx] <- 0

//                         barrier ()

//                         for j in leftPointers.[rowIdx] .. warpsInWG .. leftPointers.[rowIdx + 1] - 1 do
//                             let aCol = leftColumns.[j]
//                             let aVal = leftValues.[j]

//                             for k in rightPointers.[aCol] .. Warp .. rightPointers.[aCol + 1] - 1 do
//                                 let bCol = rightColumns.[k]
//                                 let bVal = rightValues.[k]

//                                 let key = bCol
//                                 let mutable hash = (key * HashScale) % tableSize

//                                 let mutable _break = false
//                                 while not _break do
//                                     if table.[hash] = key then
//                                         // TODO make it arbitrary (not atomic add but plus)
//                                         localValues.[hash] <- (%plus) localValues.[hash] ((%times) aVal bVal)
//                                         _break <- true
//                                     elif table.[hash] = -1 then
//                                         let old = aCompExchR table.[hash] (-1) key
//                                         if old = -1 then
//                                             // TODO make it arbitrary (not atomic add but plus)
//                                             localValues.[hash] <- (%plus) localValues.[hash] ((%times) aVal bVal)
//                                             _break <- true
//                                     else
//                                         hash <- (hash + 1) % tableSize

//                         barrier ()

//                         if lid < Warp then
//                             for i in warpLocalId .. Warp .. tableSize - 1 do
//                                 if table.[i] <> -1 then
//                                     let idx = glNz.[rowIdx] <!+> 1
//                                     table.[idx] <- table.[i]
//                                     localValues.[idx] <- localValues.[i]

//                         barrier ()

//                         let nz = glNz.[rowIdx]
//                         let globalColumnsIdx = outputPointers.[rowIdx]
//                         for i in lid .. wgSize .. nz - 1 do
//                             let target = table.[i]
//                             let mutable count = 0
//                             for k = 0 to nz - 1 do
//                                 count <- int (uint32 (table.[k]  - target) >>> 31)
//                             outputColumns.[globalColumnsIdx + count] <- table.[i]
//                             outputValues.[globalColumnsIdx + count] <- localValues.[i]
//                 @>

//             do! RunCommand kernel <| fun kernelPrepare ->
//                 kernelPrepare
//                 <| _1D(binLength * Pwarp |> Utils.getValidGlobalSize wgSize, wgSize)
//                 <| flatBins
//                 <| binsPointers.[binId]
//                 <| binLength
//                 <| glnz
//                 <| leftMatrix.RowPointers
//                 <| leftMatrix.ColumnIndices
//                 <| leftMatrix.Values
//                 <| rightMatrix.RowPointers
//                 <| rightMatrix.ColumnIndices
//                 <| rightMatrix.Values
//                 <| outputMatrix.RowPointers
//                 <| outputMatrix.ColumnIndices
//                 <| outputMatrix.Values
//         }

//         // nnz of each row in c
//         let nnzEstimation = Array.zeroCreate<int> flatBins.Length // count of rows (M)

//         for binId = 0 to BinsCount - 1 do
//             if getBinLen binId binsPointers <> 0 then // bin_size[i]
//                 if binId = 0 then
//                     do! numericPW nnzEstimation
//                 else
//                     failwith "kekw2"
//                 // elif binId <> MaxBinId then
//                 //     do! numericWG binId nnzEstimation
//                 // else
//                 //     // global
//                 //     ()
//     }
