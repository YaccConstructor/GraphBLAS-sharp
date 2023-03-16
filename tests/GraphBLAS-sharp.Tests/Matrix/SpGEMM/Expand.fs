module GraphBLAS.FSharp.Tests.Backend.Matrix.SpGEMM.Expand

open GraphBLAS.FSharp.Backend.Matrix.CSR.SpGEMM
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Test
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects.Matrix
open Expecto
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open Brahma.FSharp

/// <remarks>
/// Left matrix
/// </remarks>
/// <code>
/// [ 0 0 2 3 0
///   0 0 0 0 0
///   0 8 0 5 4
///   0 0 2 0 0
///   1 7 0 0 0 ]
/// </code>
let leftMatrix =
    { RowCount = 5
      ColumnCount = 5
      RowPointers = [| 0; 2; 2; 5; 6; 8 |]
      ColumnIndices = [| 2; 3; 1; 3; 4; 2; 0; 1 |]
      Values = [| 2; 3; 8; 5; 4; 2; 1; 7 |] }

/// <remarks>
/// Right matrix
/// </remarks>
/// <code>
/// [ 0 0 0 0 0 0 0
///   0 3 0 0 4 0 4
///   0 0 2 0 0 2 0
///   0 5 0 0 0 9 1
///   0 0 0 0 1 0 8 ]
/// </code>
let rightMatrix =
    { RowCount = 5
      ColumnCount = 7
      RowPointers = [| 0; 0; 3; 5; 8; 10 |]
      ColumnIndices = [| 1; 4; 6; 2; 5; 1; 5; 6; 4; 6 |]
      Values = [| 3; 4; 4; 2; 2; 5; 9; 1; 1; 8 |] }

type ExpandedResult<'a> =
    { Values: 'a []
      Columns: int []
      RowPointers: int [] }

let config = { Utils.defaultConfig with arbitrary = [ typeof<Generators.PairOfMatricesOfCompatibleSize> ] }

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let hostExpand multiplication (leftMatrix: Matrix.CSR<'a>) (rightMatrix: Matrix.CSR<'a>) =
    // Pointers to start positions for right matrix rows in global array
    // With duplicates which means that there is no string in the global array
    let rowsPointersToGlobalArray, globalLength =
        let requiredRightMatrixRowsLength =
            (fun index ->
                let columnIndex = leftMatrix.ColumnIndices.[index]

                let startPointer = rightMatrix.RowPointers.[columnIndex]
                let endPointer = rightMatrix.RowPointers.[columnIndex + 1]

                endPointer - startPointer)
            |> Array.init leftMatrix.ColumnIndices.Length

        //printfn "requiredRightMatrixRowsLength: %A" requiredRightMatrixRowsLength

        // Get right matrix row positions in global array by side effect
        let globalLength =
            Utils.prefixSumExclude requiredRightMatrixRowsLength 0 (+)

        //printfn "requiredRightMatrixRowsLength after prefix sum: %A" requiredRightMatrixRowsLength

        requiredRightMatrixRowsLength, globalLength

    //printfn "global length: %A" globalLength

    let resultGlobalRowPointers =
        (fun index ->
            if index < leftMatrix.RowPointers.Length - 1 then
                let rowPointer = leftMatrix.RowPointers.[index]

                // printfn "index: %A; lenght: %A" rowPointer rowsPointersToGlobalArray.Length

                rowsPointersToGlobalArray.[rowPointer]
            else
                globalLength)
        |> Array.init leftMatrix.RowPointers.Length

    // Right matrix row positions in global array without duplicates
    let globalRightMatrixRowPositions = Array.distinct rowsPointersToGlobalArray

    //printfn "global right matrix row positions without pointers: %A" globalRightMatrixRowPositions

    // Create global map
    let globalMap =
        let array =
            (fun index -> if Array.contains index globalRightMatrixRowPositions then 1 else 0)
            |> Array.init globalLength

        Utils.prefixSumInclude array 0 (+) |> ignore

        array

    //printfn "%A" globalMap

    // get required left matrix columns and values
    let requiredLeftMatrixColumns, requireLeftMatrixValues =
        let positions =
            Utils.getUniqueBitmap rowsPointersToGlobalArray

        let length = Utils.prefixSumExclude positions 0 (+)

        let requiredLeftMatrixColumns = Array.zeroCreate length

        Utils.scatter positions leftMatrix.ColumnIndices requiredLeftMatrixColumns

        // printfn "required left matrix columns: %A" requiredLeftMatrixColumns

        let requiredLeftMatrixValues = Array.zeroCreate length

        Utils.scatter positions leftMatrix.Values requiredLeftMatrixValues

        // printfn "required left matrix values: %A" requiredLeftMatrixValues

        requiredLeftMatrixColumns, requiredLeftMatrixValues

    // right matrix required row pointers
    let rightMatrixRequiredRowsPointers =
        (fun index ->
            let requiredLeftMatrixColumn = requiredLeftMatrixColumns.[index]

            rightMatrix.RowPointers.[requiredLeftMatrixColumn])
        |> Array.init globalRightMatrixRowPositions.Length

    //printfn "right matrix required row pointers: %A" rightMatrixRequiredRowsPointers

    let globalRequiredRightMatrixValuesIndices =
        (fun index ->
            let rowID = globalMap.[index] - 1
            let sourcePosition = globalRightMatrixRowPositions.[rowID]
            let offset = index - sourcePosition

            rightMatrixRequiredRowsPointers.[rowID] + offset)
        |> Array.init globalLength

    //printfn "global required right matrix values: %A" globalRequiredRightMatrixValuesIndices

    let globalRightMatrixRequiredValues =
        (fun index ->
            let valueIndex = globalRequiredRightMatrixValuesIndices.[index]
            rightMatrix.Values.[valueIndex])
        |> Array.init globalLength

    let globalRightMatrixRequiredColumnIndices =
        (fun index ->
            let valueIndex = globalRequiredRightMatrixValuesIndices.[index]
            rightMatrix.ColumnIndices.[valueIndex])
        |> Array.init globalLength

    //printfn "global required right matrix columns: %A" globalRightMatrixRequiredColumnIndices

    let globalLeftMatrixRequiredValues =
        (fun index ->
            let valueIndex = globalMap.[index] - 1

            requireLeftMatrixValues.[valueIndex])
        |> Array.init globalLength

    let resultValues =
        Array.map2 multiplication globalRightMatrixRequiredValues globalLeftMatrixRequiredValues

    { Values = resultValues
      Columns = globalRightMatrixRequiredColumnIndices
      RowPointers = resultGlobalRowPointers }

let checkResult multiplication leftMatrix rightMatrix actualResult =
    let expected =
        hostExpand multiplication leftMatrix rightMatrix

    "Values must be the same"
    |> Expect.sequenceEqual expected.Values actualResult.Values

    "Columns must be the same"
    |> Expect.sequenceEqual expected.Columns actualResult.Columns

    "Row pointers must be the same"
    |> Expect.sequenceEqual expected.RowPointers actualResult.RowPointers

    printfn "SUCCESS"

let makeTest isZero multiplication expand (leftArray: 'a [,], rightArray: 'a [,]) =

    let leftMatrix =
        Utils.createMatrixFromArray2D CSR leftArray isZero
        |> Utils.castMatrixToCSR

    let rightMatrix =
        Utils.createMatrixFromArray2D CSR rightArray isZero
        |> Utils.castMatrixToCSR

    if leftMatrix.NNZ > 0 && rightMatrix.NNZ > 0 then

        try
            //printfn $"left matrix: %A{leftArray}"
            //printfn $"right matrix: %A{rightArray}"

            if leftMatrix.ColumnCount <> rightMatrix.RowCount then
                failwith "LOLO"

            hostExpand multiplication leftMatrix rightMatrix |> ignore

            let deviceLeftMatrix =
                leftMatrix.ToDevice context

            let deviceRightMatrix =
                rightMatrix.ToDevice context

            let (multiplicationResult: ClArray<'a>),
                (extendedRightMatrixColumns: ClArray<int>),
                (resultRowPointers: ClArray<int>) =
                    expand processor deviceLeftMatrix deviceRightMatrix

            { Values = multiplicationResult.ToHostAndFree processor
              Columns = extendedRightMatrixColumns.ToHostAndFree processor
              RowPointers = resultRowPointers.ToHostAndFree processor }
            |> checkResult multiplication leftMatrix rightMatrix
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | _ -> reraise ()

let creatTest<'a when 'a : struct and 'a : equality> (isZero: 'a -> bool) multiplicationQ multiplication  =
    Expand.run context Utils.defaultWorkGroupSize multiplicationQ
    |> makeTest isZero multiplication
    |> testPropertyWithConfig config $"Expand.run on %A{typeof<'a>}"

let testFixtures =
    creatTest ((=) 0) <@ (*) @> (*)

let check =
    let leftMatrix = Utils.createMatrixFromArray2D CSR <| array2D [[-2; 3; -1; -3]; [2; -1; 3; -1]]

    let rightMatrix = Utils.createMatrixFromArray2D CSR <| array2D [[3; 0; 3; 4]; [1; -4; 1; 0]]

    ()

