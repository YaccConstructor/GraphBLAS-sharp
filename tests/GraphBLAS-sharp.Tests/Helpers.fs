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
                    typeof<Generators.PairOfSparseVectorAndMatrixOfCompatibleSize>
                    typeof<Generators.ArrayOfDistinctKeys>
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
                Actual value is %A{actual.[i]}, expected %A{expected.[i]}"
                |> failtestf "%s"

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
