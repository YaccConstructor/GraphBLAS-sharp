namespace GraphBLAS.FSharp.Backend.Quotes

open Brahma.FSharp

module BinSearch =
    let searchInRange<'a> =
        <@ fun leftEdge rightEdge sourceIndex (indices: ClArray<int>) (values: ClArray<'a>) ->

            let mutable leftEdge = leftEdge
            let mutable rightEdge = rightEdge

            let mutable result = None

            while leftEdge <= rightEdge do
                let middleIdx = (leftEdge + rightEdge) / 2

                let currentColumn = indices.[middleIdx]

                if sourceIndex = currentColumn then
                    result <- Some values.[middleIdx]

                    rightEdge <- -1 // TODO() break
                elif sourceIndex < currentColumn then
                    rightEdge <- middleIdx - 1
                else
                    leftEdge <- middleIdx + 1

            result @>

    let searchCOO<'a> =
        <@ fun lenght sourceIndex (rowIndices: ClArray<int>) (columnIndices: ClArray<int>) (values: ClArray<'a>) ->

            let mutable leftEdge = 0
            let mutable rightEdge = lenght - 1

            let mutable result = None

            while leftEdge <= rightEdge do
                let middleIdx = (leftEdge + rightEdge) / 2

                let currentIndex: uint64 =
                    ((uint64 rowIndices.[middleIdx]) <<< 32)
                    ||| (uint64 columnIndices.[middleIdx])

                if sourceIndex = currentIndex then
                    result <- Some values.[middleIdx]

                    rightEdge <- -1 // TODO() break
                elif sourceIndex < currentIndex then
                    rightEdge <- middleIdx - 1
                else
                    leftEdge <- middleIdx + 1

            result @>
