namespace GraphBLAS.FSharp.Tests

open Brahma.FSharp.OpenCL.Translator
open Microsoft.FSharp.Reflection
open Brahma.FSharp
open OpenCL.Net
open GraphBLAS.FSharp.Test
open System.Text.RegularExpressions
open Expecto
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects


[<RequireQualifiedAccess>]
module Utils =
    let defaultWorkGroupSize = 32

    let defaultConfig =
        { FsCheckConfig.defaultConfig with
              maxTest = 10
              startSize = 1
              endSize = 1000
              arbitrary =
                  [ typeof<Generators.SingleMatrix>
                    typeof<Generators.PairOfSparseMatricesOfEqualSize>
                    typeof<Generators.PairOfMatricesOfCompatibleSize>
                    typeof<Generators.PairOfSparseMatrixAndVectorsCompatibleSize>
                    typeof<Generators.PairOfSparseVectorAndMatrixAndMaskOfCompatibleSize>
                    typeof<Generators.ArrayOfDistinctKeys2D>
                    typeof<Generators.ArrayOfAscendingKeys>
                    typeof<Generators.BufferCompatibleArray>
                    typeof<Generators.PairOfVectorsOfEqualSize>
                    typeof<Generators.PairOfArraysAndValue> ] }

    let floatIsEqual x y =
        abs (x - y) < Accuracy.medium.absolute
        || x.Equals y

    let inline float32IsEqual x y =
        float (abs (x - y)) < Accuracy.medium.absolute
        || x.Equals y

    let vectorToDenseVector =
        function
        | Vector.Dense vector -> vector
        | _ -> failwith "Vector format must be Dense."

    let undirectedAlgoConfig =
        { FsCheckConfig.defaultConfig with
              maxTest = 10
              startSize = 1
              endSize = 1000
              arbitrary = [ typeof<Generators.SingleSymmetricalMatrix> ] }

    let createMatrixFromArray2D matrixCase array isZero =
        match matrixCase with
        | CSR ->
            Matrix.CSR
            <| Matrix.CSR.FromArray2D(array, isZero)
        | COO ->
            Matrix.COO
            <| Matrix.COO.FromArray2D(array, isZero)
        | CSC ->
            Matrix.CSC
            <| Matrix.CSC.FromArray2D(array, isZero)
        | LIL ->
            Matrix.LIL
            <| Matrix.LIL.FromArray2D(array, isZero)

    let createVectorFromArray vectorCase array isZero =
        match vectorCase with
        | VectorFormat.Sparse ->
            Vector.Sparse
            <| Vector.Sparse.FromArray(array, isZero)
        | VectorFormat.Dense ->
            Vector.Dense
            <| ArraysExtensions.DenseVectorFromArray(array, isZero)

    let createArrayFromDictionary size zero (dictionary: System.Collections.Generic.Dictionary<int, 'a>) =
        let array = Array.create size zero

        for key in dictionary.Keys do
            array.[key] <- dictionary.[key]

        array

    let unwrapOptionArray zero array =
        Array.map
            (fun x ->
                match x with
                | Some v -> v
                | None -> zero)
            array

    let compareArrays areEqual (actual: 'a []) (expected: 'a []) message =
        $"%s{message}. Lengths should be equal. Actual is %A{actual}, expected %A{expected}"
        |> Expect.equal actual.Length expected.Length

        for i in 0 .. actual.Length - 1 do
            if not (areEqual actual.[i] expected.[i]) then
                $"%s{message}. Arrays differ at position %A{i} of %A{actual.Length - 1}.
                Actual value is %A{actual.[i]}, expected %A{expected.[i]}, \n actual: %A{actual} \n expected: %A{expected}"
                |> failtestf "%s"

    let compareChunksArrays areEqual (actual: 'a [] []) (expected: 'a [] []) message =
        $"%s{message}. Lengths should be equal. Actual is %A{actual}, expected %A{expected}"
        |> Expect.equal actual.Length expected.Length

        for i in 0 .. actual.Length - 1 do
            compareArrays areEqual actual.[i] expected.[i] message

    let compare2DArrays areEqual (actual: 'a [,]) (expected: 'a [,]) message =
        $"%s{message}. Lengths should be equal. Actual is %A{actual}, expected %A{expected}"
        |> Expect.equal actual.Length expected.Length

        for i in 0 .. Array2D.length1 actual - 1 do
            for j in 0 .. Array2D.length2 actual - 1 do
                if not (areEqual actual.[i, j] expected.[i, j]) then
                    $"%s{message}. Arrays differ at position [%d{i}, %d{j}] of [%A{Array2D.length1 actual}, %A{Array2D.length2 actual}].
                    Actual value is %A{actual.[i, j]}, expected %A{expected.[i, j]}"
                    |> failtestf "%s"

    let compareSparseVectors isEqual (actual: Vector.Sparse<'a>) (expected: Vector.Sparse<'a>) =
        "Sparse vector size must be the same"
        |> Expect.equal actual.Size expected.Size

        "Value must be the same"
        |> compareArrays isEqual actual.Values expected.Values

        "Indices must be the same"
        |> compareArrays (=) actual.Indices expected.Indices

    let compareLILMatrix isEqual (actual: Matrix.LIL<'a>) (expected: Matrix.LIL<'a>) =
        "Column count must be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Rows count must be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        List.iter2
            (fun actualRow expected ->
                match actualRow, expected with
                | Some actualVector, Some expectedVector -> compareSparseVectors isEqual actualVector expectedVector
                | None, None -> ()
                | _ -> failwith "Rows are not matching")
        <| actual.Rows
        <| expected.Rows

    let compareCSRMatrix isEqual (actual: Matrix.CSR<'a>) (expected: Matrix.CSR<'a>) =
        "Column count must be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Rows count must be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "Values must be the same"
        |> compareArrays isEqual actual.Values expected.Values

        "Column indices must be the same"
        |> compareArrays (=) actual.ColumnIndices expected.ColumnIndices

        "Row pointers"
        |> compareArrays (=) actual.RowPointers expected.RowPointers

    let compareCOOMatrix isEqual (actual: Matrix.COO<'a>) (expected: Matrix.COO<'a>) =
        "Column count must be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Rows count must be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "Values must be the same"
        |> compareArrays isEqual actual.Values expected.Values

        "Column indices must be the same"
        |> compareArrays (=) actual.Columns expected.Columns

        "Row pointers"
        |> compareArrays (=) actual.Rows expected.Rows

    let listOfUnionCases<'a> =
        FSharpType.GetUnionCases typeof<'a>
        |> Array.map (fun caseInfo -> FSharpValue.MakeUnion(caseInfo, [||]) :?> 'a)
        |> List.ofArray

    let rec cartesian listOfLists =
        match listOfLists with
        | [ x ] -> List.fold (fun acc elem -> [ elem ] :: acc) [] x
        | h :: t ->
            List.fold
                (fun cacc celem ->
                    (List.fold (fun acc elem -> (elem :: celem) :: acc) [] h)
                    @ cacc)
                []
                (cartesian t)
        | _ -> []

    let isFloat64Available (context: ClDevice) =
        Array.contains CL_KHR_FP64 context.DeviceExtensions

    let transpose2DArray array =
        let result =
            Array2D.zeroCreate (Array2D.length2 array) (Array2D.length1 array)

        for i in 0 .. Array2D.length1 result - 1 do
            for j in 0 .. Array2D.length2 result - 1 do
                result.[i, j] <- array.[j, i]

        result

module HostPrimitives =
    let prefixSumInclude zero add array =
        Array.scan add zero array
        |> fun scanned -> scanned.[1..], Array.last scanned

    let prefixSumExclude zero add sourceArray =
        prefixSumInclude zero add sourceArray
        |> (fst >> Array.insertAt 0 zero)
        |> fun array -> Array.take sourceArray.Length array, Array.last array

    let getUniqueBitmapLastOccurrence array =
        Array.pairwise array
        |> fun pairs ->
            Array.init
                array.Length
                (fun index ->
                    if index = array.Length - 1
                       || fst pairs.[index] <> snd pairs.[index] then
                        1
                    else
                        0)

    let getUniqueBitmapFirstOccurrence (sourceArray: _ []) =
        let resultArray = Array.zeroCreate sourceArray.Length

        for i in 0 .. sourceArray.Length - 1 do
            if i = 0 || sourceArray.[i] <> sourceArray.[i - 1] then
                resultArray.[i] <- 1

        resultArray

    let getBitPositions bitmap =
        bitmap
        |> Array.mapi (fun index bit -> if bit = 1 then Some index else None)
        |> Array.choose id

    let reduceByKey keys value reduceOp =
        Array.zip keys value
        |> Array.groupBy fst
        |> Array.map
            (fun (key, array) ->
                Array.map snd array
                |> Array.reduce reduceOp
                |> fun value -> key, value)
        |> Array.unzip

    let reduceByKey2D firstKeys secondKeys values reduceOp =
        Array.zip firstKeys secondKeys
        |> fun compactedKeys -> reduceByKey compactedKeys values reduceOp
        ||> Array.map2 (fun (fst, snd) value -> fst, snd, value)
        |> Array.unzip3

    let generalScatter getBitmap (positions: int array) (values: 'a array) (resultValues: 'a array) =

        if positions.Length <> values.Length then
            failwith "Lengths must be the same"

        let bitmap = getBitmap positions

        Array.iteri2
            (fun index bit key ->
                if bit = 1 && 0 <= key && key < resultValues.Length then
                    resultValues.[key] <- values.[index])
            bitmap
            positions

        resultValues

    let scatterLastOccurrence positions =
        generalScatter getUniqueBitmapLastOccurrence positions

    let scatterFirstOccurrence positions =
        generalScatter getUniqueBitmapFirstOccurrence positions

    let gather (positions: int []) (values: 'a []) (result: 'a []) =
        if positions.Length <> result.Length then
            failwith "Lengths must be the same"

        Array.iteri
            (fun index position ->
                if position >= 0 && position < values.Length then
                    result.[index] <- values.[position])
            positions

        result

    let array2DKroneckerProduct leftMatrix rightMatrix op =
        Array2D.init
        <| (Array2D.length1 leftMatrix)
           * (Array2D.length1 rightMatrix)
        <| (Array2D.length2 leftMatrix)
           * (Array2D.length2 rightMatrix)
        <| fun i j ->
            let leftElement =
                leftMatrix.[i / (Array2D.length1 rightMatrix), j / (Array2D.length2 rightMatrix)]

            let rightElement =
                rightMatrix.[i % (Array2D.length1 rightMatrix), j % (Array2D.length2 rightMatrix)]

            op leftElement rightElement

    let array2DMultiplication zero mul add leftArray rightArray =
        if Array2D.length2 leftArray
           <> Array2D.length1 rightArray then
            failwith "Incompatible matrices"

        let add left right =
            match left, right with
            | Some left, Some right -> add left right
            | Some value, None
            | None, Some value -> Some value
            | _ -> None

        Array2D.init
        <| Array2D.length1 leftArray
        <| Array2D.length2 rightArray
        <| fun i j ->
            (leftArray.[i, *], rightArray.[*, j])
            // multiply and filter
            ||> Array.map2 mul
            |> Array.choose id
            // add and filter
            |> Array.map Some
            |> Array.fold add None
            |> Option.defaultValue zero

    let scanByKey scan keysAndValues =
        Array.groupBy fst keysAndValues
        |> Array.map (fun (_, array) -> Array.map snd array |> scan |> fst)
        |> Array.concat

module Context =
    type TestContext =
        { ClContext: ClContext
          Queue: MailboxProcessor<Msg> }

    let availableContexts (platformRegex: string) =
        let mutable e = ErrorCode.Unknown

        Cl.GetPlatformIDs &e
        |> Array.collect (fun platform -> Cl.GetDeviceIDs(platform, DeviceType.All, &e))
        |> Seq.ofArray
        |> Seq.distinctBy
            (fun device ->
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Name, &e)
                    .ToString())
        |> Seq.filter
            (fun device ->
                let isAvailable =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Available, &e)
                        .CastTo<bool>()

                let platform =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Platform, &e)
                        .CastTo<Platform>()

                let platformName =
                    Cl
                        .GetPlatformInfo(platform, PlatformInfo.Name, &e)
                        .ToString()

                (Regex platformRegex).IsMatch platformName
                && isAvailable)
        |> Seq.map
            (fun device ->
                let platform =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Platform, &e)
                        .CastTo<Platform>()

                let clPlatform =
                    Cl
                        .GetPlatformInfo(platform, PlatformInfo.Name, &e)
                        .ToString()
                    |> Platform.Custom

                let deviceType =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Type, &e)
                        .CastTo<DeviceType>()

                let _ =
                    match deviceType with
                    | DeviceType.Cpu -> ClDeviceType.Cpu
                    | DeviceType.Gpu -> ClDeviceType.Gpu
                    | DeviceType.Default -> ClDeviceType.Default
                    | _ -> failwith "Unsupported"

                let device =
                    ClDevice.GetFirstAppropriateDevice(clPlatform)

                let translator = FSQuotationToOpenCLTranslator device

                let context = ClContext(device, translator)
                let queue = context.QueueProvider.CreateQueue()

                { ClContext = context; Queue = queue })

    let defaultContext =
        let device = ClDevice.GetFirstAppropriateDevice()

        let context =
            ClContext(device, FSQuotationToOpenCLTranslator device)

        let queue = context.QueueProvider.CreateQueue()

        { ClContext = context; Queue = queue }

    let gpuOnlyContextFilter =
        Seq.filter
            (fun (context: TestContext) ->
                let mutable e = ErrorCode.Unknown
                let device = context.ClContext.ClDevice.Device

                let deviceType =
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Type, &e)
                        .CastTo<DeviceType>()

                deviceType = DeviceType.Gpu)

module TestCases =

    type OperationCase<'a> =
        { TestContext: Context.TestContext
          Format: 'a }

    let defaultPlatformRegex = ""

    let testCases contextFilter =
        Context.availableContexts defaultPlatformRegex
        |> contextFilter
        |> List.ofSeq

    let getTestCases<'a> contextFilter =
        Context.availableContexts defaultPlatformRegex
        |> contextFilter
        |> List.ofSeq
        |> List.collect
            (fun x ->
                Utils.listOfUnionCases<'a>
                |> List.ofSeq
                |> List.map (fun y -> x, y))
        |> List.map
            (fun pair ->
                { TestContext = fst pair
                  Format = snd pair })

    let operationGPUTests name (testFixtures: OperationCase<'a> -> Test list) =
        getTestCases<'a> Context.gpuOnlyContextFilter
        |> List.distinctBy (fun case -> case.TestContext.ClContext, case.Format)
        |> List.collect testFixtures
        |> testList name

    let gpuTests name testFixtures =
        testCases Context.gpuOnlyContextFilter
        |> List.distinctBy (fun testContext -> testContext.ClContext)
        |> List.collect testFixtures
        |> testList name
