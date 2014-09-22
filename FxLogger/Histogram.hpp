/************************************************
 * Copyright (c) IBM Corp. 2014
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *************************************************/
/*
 * Contributors:
 * Blake Fitch bgf@us.ibm.com
 *
 */
#ifndef __HISTROGRAM_HPP__
#define __HISTROGRAM_HPP__

#include <FxLogger.hpp>

#ifdef HISTOGRAM_ON
template< int HistOnFlag >
class histogram_t
{
  int                 mBucketCount;
  
  unsigned long long* mCountBins;
  unsigned long long* mValueBins;

  unsigned long long  mDataPointCount;
  unsigned long long  mDataPointValueCount;

  long long           mLow;
  long long           mHigh;
  long long           mRange;

  double              mBucketReciprocal;

  char*               mName;

  // Out of range for counting bins  
  unsigned long long mOutOfRangeLowCBin;
  unsigned long long mOutOfRangeHighCBin;

  // Out of range for value bins
  unsigned long long mOutOfRangeLowVBin;
  unsigned long long mOutOfRangeHighVBin;

  // Lets the called to associate a number 
  // with a histogram
  
public:
  
  void
  Reset()
  {
    if( HistOnFlag )
      {      
        mDataPointCount = 0;
        mDataPointValueCount = 0;
        
        for( int i = 0; i < mBucketCount; i++)
          {
            mCountBins[ i ] = 0;
            mValueBins[ i ] = 0;
          }
        
        mOutOfRangeLowCBin = 0;
        mOutOfRangeHighCBin = 0;
        mOutOfRangeLowVBin = 0;
        mOutOfRangeHighVBin = 0;        
      }
  }

  void
  Finalize()
  {
    if( HistOnFlag )
      {
        if( mCountBins != NULL )
          {
            free( mCountBins );
            mCountBins = NULL;
          }

        if( mValueBins != NULL )
          {
            free( mValueBins );
            mValueBins = NULL;
          }
      }
  }
  
  void Init( const char* aName, long long Low, long long High, int aBucketCount )
  {
    if( HistOnFlag )
      {
        mBucketCount = aBucketCount;

        mName = aName;
    
        mLow  = Low;
        mHigh = High;
        mRange = mHigh - mLow;
    
        mBucketReciprocal = 1.0 * mBucketCount / mRange;
    
        int BinsSize = sizeof( unsigned long long ) * mBucketCount;

        mCountBins = (unsigned long long *) malloc( BinsSize );
        StrongAssertLogLine( mCountBins )
          << "histrogram_t::Init(): ERROR: Not enough memory for"
          << " BinsSize: " << BinsSize
          << EndLogLine;

        mValueBins = (unsigned long long *) malloc( BinsSize );
        StrongAssertLogLine( mValueBins )
          << "histrogram_t::Init(): ERROR: Not enough memory for"
          << " BinsSize: " << BinsSize
          << EndLogLine;

        Reset();
      }
  }
  
  void
  Add( long long aValue )
  {
    if( HistOnFlag )
      {
        if(( aValue - mLow ) < 0 )
          {
            mOutOfRangeLowCBin++;
            mOutOfRangeLowVBin += aValue;
            mDataPointCount++;
            mDataPointValueCount += aValue;
            return;
          }
      
        int BinIndex = (int)( (aValue-mLow) * mBucketReciprocal);
    
        if( BinIndex >= 0 && BinIndex < mBucketCount )
          {
            mCountBins[ BinIndex ]++;
            mValueBins[ BinIndex ] += aValue;
          }
        else if( BinIndex >= mBucketCount )
          {
            mOutOfRangeHighCBin++;
            mOutOfRangeHighVBin += aValue;
          }
        else 
          StrongAssertLogLine( 0 )
            << "histrogram_t::Add(): "
            << " BinIndex: " << BinIndex
            << " mBucketReciprocal: " << mBucketReciprocal
            << " aValue: " << aValue
            << " mLow: " << mLow
            << " mDataPointCount: " << mDataPointCount
            << " mDataPointValueCount: " << mDataPointValueCount
            << EndLogLine;
    
        mDataPointCount++;
        mDataPointValueCount += aValue;
      }
  }

  void Report()
  {
    if( HistOnFlag )
      {
        BegLogLine( 1 )
          << "histogram_t(): "
          << " Name: " << mName
          << " mLow: " << mLow
          << " mHigh: " << mHigh
          << " mBucketCount: " << mBucketCount
          << " mDataPointCount: " << mDataPointCount
          << " mDataPointValueCount: " << mDataPointValueCount
          << EndLogLine;    

        double CountsPerBucket = 1.0 / mBucketReciprocal;

        BegLogLine( 1 )
          << "histogram_t(): "
          << " Name: " << mName
          << " mOutOfRangeLowCBin: " << mOutOfRangeLowCBin
          << " mOutOfRangeHighCBin: " << mOutOfRangeHighCBin
          << EndLogLine;

        BegLogLine( 1 )
          << "histogram_t(): "
          << " Name: " << mName
          << " mOutOfRangeLowVBin: " << mOutOfRangeLowVBin
          << " mOutOfRangeHighVBin: " << mOutOfRangeHighVBin
          << EndLogLine;
        
        for( int i = 0; i < mBucketCount; i++)
          {
            BegLogLine( 1 )
              << "histogram_t(): "
              << " Name: " << mName
              << " Interval: { " <<  (long long) (CountsPerBucket*i + mLow) << " , " 
              << (long long) (CountsPerBucket * (i+1) + mLow ) << " } "
              << " CountBin[ " << i << " ]: " << mCountBins[ i ]
              << " ValueBin[ " << i << " ]: " << mValueBins[ i ]
              << EndLogLine;
          }         
      }
  }
};
#else
template< int HistOnFlag >
class histogram_t
{  
public:
  
  void
  Reset(int aId) {}

  void
  Finalize() {}
  
  void Init( const char* aName, long long Low, long long High, int aBucketCount ) {}
  
  void
  Add( long long aValue ) {}

  void Report() {}
};
#endif
#endif
