/*
 * JBoss, a division of Red Hat
 * Copyright 2010, Red Hat Middleware, LLC, and individual
 * contributors as indicated by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.gatein.wsrp.consumer;

import org.gatein.common.util.ParameterValidation;
import org.gatein.common.util.Version;
import org.gatein.pc.api.InvokerUnavailableException;
import org.gatein.wsrp.services.MarkupService;
import org.gatein.wsrp.services.PortletManagementService;
import org.gatein.wsrp.services.RegistrationService;
import org.gatein.wsrp.services.SOAPServiceFactory;
import org.gatein.wsrp.services.ServiceDescriptionService;
import org.gatein.wsrp.services.ServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the information pertaining to the web service connection to the remote producer via its {@link ServiceFactory} and provides access to the services classes for WSRP
 * invocations.
 *
 * @author <a href="mailto:chris.laprun@jboss.com">Chris Laprun</a>
 * @version $Revision: 13122 $
 * @since 2.6
 */
public class EndpointConfigurationInfo
{
   private static final Logger log = LoggerFactory.getLogger(EndpointConfigurationInfo.class);

   // transient variables
   /** Access to the WS */
   private transient ServiceFactory serviceFactory;
   private transient String remoteHostAddress;
   private transient boolean started;

   /**
    * Used to implement a simple round-robin-like mechanism to switch producer URL in case one is down
    */
   private transient String[] allWSDLURLs;
   private transient int currentURL;
   private transient long lastThroughListTime = System.currentTimeMillis();
   private static final String SEPARATOR = " ";

   public EndpointConfigurationInfo()
   {
      serviceFactory = new SOAPServiceFactory();
   }

   public EndpointConfigurationInfo(ServiceFactory serviceFactory)
   {
      ParameterValidation.throwIllegalArgExceptionIfNull(serviceFactory, "ServiceFactory");
      this.serviceFactory = serviceFactory;
   }

   public String getWsdlDefinitionURL()
   {
      return serviceFactory.getWsdlDefinitionURL();
   }

   public void setWsdlDefinitionURL(String wsdlDefinitionURL)
   {
      if (wsdlDefinitionURL != null && wsdlDefinitionURL.contains(SEPARATOR))
      {
         // we have a URL with a separator to support passing several URLs for a simple failover mechanism, so we need to extract the individual URLs
         allWSDLURLs = wsdlDefinitionURL.split("\\s+");
         currentURL = 0;
         wsdlDefinitionURL = allWSDLURLs[currentURL];
      }
      serviceFactory.setWsdlDefinitionURL(wsdlDefinitionURL);
   }


   public String[] getAllWSDLURLs()
   {
      return allWSDLURLs;
   }

   public void start() throws Exception
   {
      if (!started)
      {
         try
         {
            serviceFactory.start();
         }
         catch (Exception e)
         {
            if (allWSDLURLs != null)
            {
               // we have a list of alternate URLs, try them in order first
               do
               {
                  // increment pointer to current URL
                  currentURL++;
                  // if we are moving past the last element, loop on the list only if we haven't looped through it in the last msBeforeTimeOut milliseconds (to avoid infinite loop)
                  if (currentURL == allWSDLURLs.length)
                  {
                     currentURL = 0;
                     final long now = System.currentTimeMillis();
                     final long delta = now - lastThroughListTime;
                     lastThroughListTime = now;
                     final int msBeforeTimeOut = serviceFactory.getWSOperationTimeOut();
                     if (delta < msBeforeTimeOut)
                     {
                        log.info("SOAPServiceFactory looped through all available WSDL URLs in the last " + msBeforeTimeOut
                           + " milliseconds. We're considering that producers haven't had time to start again in that meantime so failing to avoid looping indefinitely.");
                        break;
                     }
                  }

                  // check the new WSDL URL
                  String old = serviceFactory.getWsdlDefinitionURL();
                  final String wsdlDefinitionURL = allWSDLURLs[currentURL];
                  serviceFactory.setWsdlDefinitionURL(wsdlDefinitionURL);
                  log.info("Couldn't access WSDL information at " + old + ". Attempting to use next URL (" + wsdlDefinitionURL + ") in the list", e);
                  try
                  {
                     start();
                     break; // if start was successful, exit the loop!
                  }
                  catch (Exception e1)
                  {
                     // start failed again, just keep on looping
                  }
               }
               while (true);
            }
            else
            {
               throw new RuntimeException(e);
            }
         }
         started = true;
      }
   }

   public void stop() throws Exception
   {
      if (started)
      {
         serviceFactory.stop();
         started = false;
      }
   }

   ServiceFactory getServiceFactory()
   {
      try
      {
         start();
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
      return serviceFactory;
   }

   ServiceDescriptionService getServiceDescriptionService() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().getServiceDescriptionService();
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException("Couldn't access ServiceDescription service. Cause: "
            + e.getLocalizedMessage(), e);
      }
   }

   MarkupService getMarkupService() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().getMarkupService();
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException("Couldn't access Markup service. Cause: "
            + e.getLocalizedMessage(), e);
      }
   }

   PortletManagementService getPortletManagementService() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().getPortletManagementService();
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException("Couldn't access PortletManagement service. Cause: "
            + e.getLocalizedMessage(), e);
      }
   }

   RegistrationService getRegistrationService() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().getRegistrationService();
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException("Couldn't access Registration service. Cause: "
            + e.getLocalizedMessage(), e);
      }
   }

   public boolean isAvailable()
   {
      return serviceFactory.isAvailable();
   }

   public boolean isRefreshNeeded()
   {
      boolean result = !isAvailable();
      if (result && log.isDebugEnabled())
      {
         log.debug("Refresh needed");
      }
      return result;
   }

   public boolean refresh() throws InvokerUnavailableException
   {
      return isRefreshNeeded() && forceRefresh();
   }

   boolean forceRefresh() throws InvokerUnavailableException
   {
      try
      {
         return getServiceFactory().refresh(true);
      }
      catch (Exception e)
      {
         throw new InvokerUnavailableException(e);
      }
   }

   public String getRemoteHostAddress()
   {
      if (remoteHostAddress == null)
      {
         // extract host URL
         String wsdl = getWsdlDefinitionURL();
         int hostBegin = wsdl.indexOf("://") + 3;
         remoteHostAddress = wsdl.substring(0, wsdl.indexOf('/', hostBegin));
      }

      return remoteHostAddress;
   }

   /**
    * Number of milliseconds before a WS operation is considered as having timed out.
    *
    * @param msBeforeTimeOut number of milliseconds to wait for a WS operation to return before timing out. Will be set
    *                        to {@link ServiceFactory#DEFAULT_TIMEOUT_MS} if negative.
    */
   public void setWSOperationTimeOut(int msBeforeTimeOut)
   {
      serviceFactory.setWSOperationTimeOut(msBeforeTimeOut);
   }

   public int getWSOperationTimeOut()
   {
      return serviceFactory.getWSOperationTimeOut();
   }

   Version getWSRPVersion()
   {
      return serviceFactory.getWSRPVersion();
   }

   public boolean getWSSEnabled()
   {
      return serviceFactory.isWSSEnabled();
   }

   public void setWSSEnabled(boolean enable)
   {
      serviceFactory.enableWSS(enable);
   }

   public boolean isWSSAvailable()
   {
      return serviceFactory.isWSSAvailable();
   }
}
