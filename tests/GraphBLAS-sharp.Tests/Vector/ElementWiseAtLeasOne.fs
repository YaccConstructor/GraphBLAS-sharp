module Backend.Vector.ElementWiseAtLeastOne

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

let checkResult
    (isEqual: 'c -> 'c -> bool)
    resultZero
    (op: 'a -> 'b -> 'c)
    (actual: Vector<'c>)
    (leftArray: 'a [])
    (rightArray: 'b [])
    =

    let expectedArrayLength = leftArray.Length

    let expectedArray =
        Array.create expectedArrayLength resultZero

    for i in 0 .. expectedArrayLength - 1 do
        expectedArray.[i] <- op leftArray.[i] rightArray.[i]

    match actual with
    | VectorSparse actual ->
        let actualArray =
            Array.create expectedArrayLength resultZero

        for i in 0 .. actual.Indices.Length - 1 do
            if isEqual actual.Values.[i] resultZero then
                failwith "Resulting zeroes should be filtered."

            actualArray.[actual.Indices.[i]] <- actual.Values.[i]

        $"arrays must have the same values actual = %A{actualArray}, expected = %A{expectedArray}"
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
    case
    (leftArray: 'a [], rightArray: 'b [])
    =

    let leftNNZCount =
        NNZCountCount leftArray (leftIsEqual leftZero)

    let rightNNZCount =
        NNZCountCount rightArray (rightIsEqual rightZero)

    if leftNNZCount > 0 && rightNNZCount > 0 then

        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let firstVector =
            createVectorFromArray case.Format leftArray (leftIsEqual leftZero)

        let secondVector =
            createVectorFromArray case.Format rightArray (rightIsEqual rightZero)

        let v1 = firstVector.ToDevice context
        let v2 = secondVector.ToDevice context

        try
            let res = addFun q v1 v2

            v1.Dispose q
            v2.Dispose q

            let cooRes = toCoo q res
            res.Dispose q

            let actual = cooRes.ToHost q

            checkResult resultIsEqual resultZero op actual leftArray rightArray
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let addTestFixtures case =
    let config = defaultConfig

    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on AtLeastOne<{fstType}, {sndType}> -> {thrType} option, {case.Format}"

    let wgSize = 32
    let context = case.ClContext.ClContext

    [ let toCoo = Vector.toSparse context wgSize

      let intAddFun =
          Vector.elementWiseAtLeastOne context intSumAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (+) intAddFun toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatToCoo = Vector.toSparse context wgSize

      let floatAddFun =
          Vector.elementWiseAtLeastOne context floatSumAtLeastOne wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      case
      |> correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (+) floatAddFun floatToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolToCoo = Vector.toSparse context wgSize

      let boolAddFun =
          Vector.elementWiseAtLeastOne context boolSumAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) false false false (||) boolAddFun boolToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteToCoo = Vector.toSparse context wgSize

      let byteAddFun =
          Vector.elementWiseAtLeastOne context byteSumAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (+) byteAddFun byteToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let addTests =
    testsWithOperationCase<VectorFormat> addTestFixtures "Backend.Vector.ElementWiseAtLeasOneAdd tests"

let mulTestFixtures case =
    let config = defaultConfig

    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on AtLeastOne<{fstType}, {sndType}> -> {thrType} option, {case.Format}"

    let wgSize = 32
    let context = case.ClContext.ClContext


    [ let toCoo = Vector.toSparse context wgSize

      let intMulFun =
          Vector.elementWiseAtLeastOne context intMulAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0 0 0 (*) intMulFun toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatToCoo = Vector.toSparse context wgSize

      let floatMulFun =
          Vector.elementWiseAtLeastOne context floatMulAtLeastOne wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      case
      |> correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (*) floatMulFun floatToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolToCoo = Vector.toSparse context wgSize

      let boolMulFun =
          Vector.elementWiseAtLeastOne context boolMulAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) false false false (&&) boolMulFun boolToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteToCoo = Vector.toSparse context wgSize

      let byteMulFun =
          Vector.elementWiseAtLeastOne context byteMulAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (*) byteMulFun byteToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let mulTests =
    testsWithOperationCase<VectorFormat> mulTestFixtures "Backend.Vector.ElementWiseAtLeasOneMul tests"

