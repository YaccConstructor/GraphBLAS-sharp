module Backend.Vector.ElementWiseAddAtLeastOne

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend.Common
open StandardOperations
open OpenCL.Net

let logger =
    Log.create "Vector.ElementWiseAtLeasOneMul.Tests"

let NNZCountCount array isZero =
    Array.filter (fun item -> not <| isZero item) array
    |> Array.length

let fFilter =
    fun item ->
        System.Double.IsNaN item
        || System.Double.IsInfinity item
    >> not
    |> Array.filter

let checkResult
    (isEqual: 'c -> 'c -> bool)
    leftZero
    rightZero
    resultZero
    (op: 'a -> 'b -> 'c)
    (actual: Vector<'c>)
    (leftArray: 'a [])
    (rightArray: 'b [])
    =

    let expectedArrayLength = max leftArray.Length rightArray.Length

    let isLeftLess = leftArray.Length < rightArray.Length

    let lowBound =
        if isLeftLess then
            leftArray.Length
        else
            rightArray.Length

    let expectedArray =
        Array.create expectedArrayLength resultZero

    for i in 0 .. expectedArrayLength - 1 do
        let item =
            if i < lowBound then
                op leftArray.[i] rightArray.[i]
            elif isLeftLess then
                op leftZero rightArray.[i]
            else
                op leftArray.[i] rightZero

        expectedArray.[i] <- item

    match actual with
    | VectorSparse actual ->
        let actualArray =
            Array.create expectedArrayLength resultZero

        for i in 0 .. actual.Indices.Length - 1 do
            if isEqual actual.Values.[i] resultZero then
                failwith "Resulting zeroes should be filtered."

            actualArray.[actual.Indices.[i]] <- actual.Values.[i]

        "arrays must have the same values"
        |> compareArrays isEqual actualArray expectedArray
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest
    leftIsEqual
    rightIsEqual
    resultIsEqual
    leftZero
    rightZero
    resultZero
    op
    (addFun: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'b> -> ClVector<'c>)
    (toCoo: MailboxProcessor<_> -> ClVector<'c> -> ClVector<'c>)
    leftFilter
    rightFilter
    case
    (leftArray: 'a [])
    (rightArray: 'b [])
    =

    let leftArray = leftFilter leftArray
    let rightArray = rightFilter rightArray

    let leftNNZCount =
        NNZCountCount leftArray (leftIsEqual leftZero)

    let rightNNZCount =
        NNZCountCount rightArray (rightIsEqual rightZero)

    if leftNNZCount > 0 && rightNNZCount > 0 then

        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let firstVector =
            createVectorFromArray case.FormatCase leftArray (leftIsEqual leftZero)

        let secondVector =
            createVectorFromArray case.FormatCase rightArray (rightIsEqual rightZero)

        let v1 = firstVector.ToDevice context
        let v2 = secondVector.ToDevice context

        try
            let res = addFun q v1 v2

            v1.Dispose q
            v2.Dispose q

            let cooRes = toCoo q res
            res.Dispose q

            let actual = cooRes.ToHost q

            checkResult resultIsEqual leftZero rightZero resultZero op actual leftArray rightArray
        with
        | :? OpenCL.Net.Cl.Exception as ex -> logger.debug (eventX $"exception: {ex.Message}")

let addTestFixtures case =
    let config = defaultConfig

    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on AtLeastOne<{fstType}, {sndType}> -> {thrType} option, {case.FormatCase}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    [ let toCoo = Vector.toCoo context wgSize

      let intAddFun =
          Vector.elementWiseAddAtLeastOne context intSumAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (+) intAddFun toCoo id id
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let toFloatCoo = Vector.toCoo context wgSize

      let floatAddFun =
          Vector.elementWiseAddAtLeastOne context floatSumAtLeastOne wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      case
      |> correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (+) floatAddFun toFloatCoo fFilter fFilter
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolToCoo = Vector.toCoo context wgSize

      let boolAddFun =
          Vector.elementWiseAddAtLeastOne context boolSumAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) false false false (||) boolAddFun boolToCoo id id
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteToCoo = Vector.toCoo context wgSize

      let byteAddFun =
          Vector.elementWiseAddAtLeastOne context byteSumAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (+) byteAddFun byteToCoo id id
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let addTests =
    testCases<VectorFormat>
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect addTestFixtures
    |> testList "Backend.Vector.ElementWiseAtLeasOneAdd tests"

let mulTestFixtures case =
    let config = defaultConfig

    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on AtLeastOne<{fstType}, {sndType}> -> {thrType} option, {case.FormatCase}"

    let wgSize = 32
    let context = case.ClContext.ClContext


    [ let toCoo = Vector.toCoo context wgSize

      let intMulFun =
          Vector.elementWiseAddAtLeastOne context intMulAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (*) intMulFun toCoo id id
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let toFloatCoo = Vector.toCoo context wgSize

      let floatMulFun =
          Vector.elementWiseAddAtLeastOne context floatMulAtLeastOne wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      case
      |> correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (*) floatMulFun toFloatCoo fFilter fFilter
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolToCoo = Vector.toCoo context wgSize

      let boolMulFun =
          Vector.elementWiseAddAtLeastOne context boolMulAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) false false false (&&) boolMulFun boolToCoo id id
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteToCoo = Vector.toCoo context wgSize

      let byteMulFun =
          Vector.elementWiseAddAtLeastOne context byteMulAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (*) byteMulFun byteToCoo id id
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let mulTests =
    testCases<VectorFormat>
    |> List.filter
        (fun case ->
            let mutable e = ErrorCode.Unknown
            let device = case.ClContext.ClContext.ClDevice.Device

            let deviceType =
                Cl
                    .GetDeviceInfo(device, DeviceInfo.Type, &e)
                    .CastTo<DeviceType>()

            deviceType = DeviceType.Gpu)
    |> List.distinctBy (fun case -> case.ClContext.ClContext.ClDevice.DeviceType, case.FormatCase)
    |> List.collect mulTestFixtures
    |> testList "Backend.Vector.ElementWiseAtLeasOneMul tests"
