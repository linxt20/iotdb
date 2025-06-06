/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.udf;

import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UDFInformation {

  private String functionName;
  private String className;
  private UDFType udfType;

  // jarName and jarMD5 are null if isUsingURI is false
  private boolean isUsingURI;
  private String jarName;
  private String jarMD5;

  private UDFInformation() {}

  public UDFInformation(
      String functionName,
      String className,
      UDFType udfType,
      boolean isUsingURI,
      String jarName,
      String jarMD5) {
    this.functionName = functionName.toUpperCase();
    this.className = className;
    this.isUsingURI = isUsingURI;
    this.jarName = jarName;
    this.jarMD5 = jarMD5;
    this.udfType = udfType;
  }

  // Only used for built-in UDF
  public UDFInformation(String functionName, String className, UDFType udfType) {
    this.functionName = functionName.toUpperCase();
    this.className = className;
    this.udfType = udfType;
    this.isUsingURI = false;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getClassName() {
    return className;
  }

  public UDFType getUdfType() {
    return udfType;
  }

  public String getJarName() {
    return jarName;
  }

  public String getJarMD5() {
    return jarMD5;
  }

  public boolean isUsingURI() {
    return isUsingURI;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName.toUpperCase();
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public void setUdfType(UDFType udfType) {
    this.udfType = udfType;
  }

  public void setJarName(String jarName) {
    this.jarName = jarName;
  }

  public void setJarMD5(String jarMD5) {
    this.jarMD5 = jarMD5;
  }

  public void setUsingURI(boolean usingURI) {
    isUsingURI = usingURI;
  }

  public void setAvailable(boolean available) {
    this.udfType = this.udfType.setAvailable(available);
  }

  public boolean isAvailable() {
    return udfType.isAvailable();
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    serialize(outputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  public void serialize(DataOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(functionName, outputStream);
    ReadWriteIOUtils.write(className, outputStream);
    udfType.serialize(outputStream);
    ReadWriteIOUtils.write(isUsingURI, outputStream);
    if (isUsingURI) {
      ReadWriteIOUtils.write(jarName, outputStream);
      ReadWriteIOUtils.write(jarMD5, outputStream);
    }
  }

  public static UDFInformation deserialize(ByteBuffer byteBuffer) {
    UDFInformation udfInformation = new UDFInformation();
    udfInformation.setFunctionName(ReadWriteIOUtils.readString(byteBuffer));
    udfInformation.setClassName(ReadWriteIOUtils.readString(byteBuffer));
    udfInformation.setUdfType(UDFType.deserialize(byteBuffer));
    boolean isUsingURI = ReadWriteIOUtils.readBool(byteBuffer);
    udfInformation.setUsingURI(isUsingURI);
    if (isUsingURI) {
      udfInformation.setJarName(ReadWriteIOUtils.readString(byteBuffer));
      udfInformation.setJarMD5(ReadWriteIOUtils.readString(byteBuffer));
    }
    return udfInformation;
  }

  public static UDFInformation deserialize(InputStream inputStream) throws IOException {
    return deserialize(
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(inputStream)));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UDFInformation that = (UDFInformation) o;
    return udfType.equals(that.udfType)
        && Objects.equals(functionName, that.functionName)
        && Objects.equals(className, that.className)
        && Objects.equals(jarName, that.jarName)
        && Objects.equals(jarMD5, that.jarMD5);
  }

  @Override
  public int hashCode() {
    return Objects.hash(functionName);
  }
}
