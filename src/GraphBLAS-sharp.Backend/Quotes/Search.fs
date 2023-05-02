namespace GraphBLAS.FSharp.Backend.Quotes

open Brahma.FSharp

module Search =
    module Bin =
        /// <summary>
        /// Searches a section of the array of indices, bounded by the given left and right edges, for an index, using a binary search algorithm.
        /// In case searched section contains source index, the value at the same position in the array of values is returned.
        /// </summary>
        /// <remarks>
        /// Searched section of index array should be sorted in ascending order.
        /// The index array should have the same length as the array of values.
        /// left edge and right edge should be less than the length of the index array.
        /// </remarks>
        let inRange<'a> =
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

        /// <summary>
        /// Searches value in array by key.
        /// In case there is a value at the given key position, it is returned.
        /// </summary>
        let byKey<'a> =
            <@ fun lenght sourceIndex (keys: ClArray<int>) (values: ClArray<'a>) ->

                let mutable leftEdge = 0
                let mutable rightEdge = lenght - 1

                let mutable result = None

                while leftEdge <= rightEdge do
                    let middleIdx = (leftEdge + rightEdge) / 2
                    let currentIndex = keys.[middleIdx]

                    if sourceIndex = currentIndex then
                        result <- Some values.[middleIdx]

                        rightEdge <- -1 // TODO() break
                    elif sourceIndex < currentIndex then
                        rightEdge <- middleIdx - 1
                    else
                        leftEdge <- middleIdx + 1

                result @>

        /// <summary>
        /// Searches value in array by two keys.
        /// In case there is a value at the given keys position, it is returned.
        /// </summary>
        let byKey2D<'a> =
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

        /// <summary>
        /// Find lower position of item in array.
        /// </summary>
        let lowerPosition<'a when 'a: equality and 'a: comparison> =
            <@ fun lenght sourceItem (keys: 'a []) ->

                let mutable leftEdge = 0
                let mutable rightEdge = lenght - 1

                let mutable resultPosition = None

                while leftEdge <= rightEdge do
                    let currentPosition = (leftEdge + rightEdge) / 2
                    let currentKey = keys.[currentPosition]

                    if sourceItem = currentKey then
                        // remember positions and move left
                        resultPosition <- Some currentPosition

                        rightEdge <- currentPosition - 1
                    elif sourceItem < currentKey then
                        rightEdge <- currentPosition - 1
                    else
                        leftEdge <- currentPosition + 1

                resultPosition @>

        /// <summary>
        /// lowerBound is a version of binary search: it attempts to find the element value in an ordered range [first, last).
        /// Specifically, it returns the last position where value could be inserted without violating the ordering.
        /// </summary>
        /// <example>
        /// <code>
        /// let array = [ 0; 2; 5; 7; 8; ]
        ///
        /// lowerBound array 0 // return 1
        /// lowerBound array 1 // return 1
        /// lowerBound array 2 // return 2
        /// lowerBound array 3 // return 2
        /// lowerBound array 8 // return array.Length - 1
        /// lowerBound array 9 // return array.Length - 1
        /// </code>
        /// </example>
        let lowerBound<'a when 'a: comparison> =
            <@ fun lenght sourceItem (keys: ClArray<'a>) ->

                let mutable leftEdge = 0
                let mutable rightEdge = lenght - 1

                let mutable resultPosition = 0

                if sourceItem >= keys.[lenght - 1] then
                    lenght - 1
                else
                    while leftEdge <= rightEdge do
                        let currentPosition = (leftEdge + rightEdge) / 2
                        let currentKey = keys.[currentPosition]

                        if sourceItem < currentKey then
                            resultPosition <- currentPosition

                            rightEdge <- currentPosition - 1
                        else
                            leftEdge <- currentPosition + 1

                    resultPosition @>

        let lowerBoundAndValue<'a when 'a: comparison> =
            let defaultValue = Unchecked.defaultof<'a>

            <@ fun lenght sourceItem (keys: ClArray<'a>) ->

                let mutable leftEdge = 0
                let mutable rightEdge = lenght - 1

                let mutable resultPosition = 0, defaultValue

                if sourceItem >= keys.[lenght - 1] then
                    (lenght - 1), keys.[lenght - 1]
                else
                    while leftEdge <= rightEdge do
                        let currentPosition = (leftEdge + rightEdge) / 2
                        let currentKey = keys.[currentPosition]

                        if sourceItem < currentKey then
                            resultPosition <- currentPosition, currentKey

                            rightEdge <- currentPosition - 1
                        else
                            leftEdge <- currentPosition + 1

                    resultPosition @>
